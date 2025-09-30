package consumers

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
	"github.com/xrpscan/platform/config"
	"github.com/xrpscan/platform/connections"
	"github.com/xrpscan/platform/indexer"
	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/platform/models"
)

var emittedAssets sync.Map

func idTx(hash string) string { return uuid.NewSHA1(uuid.NameSpaceURL, []byte("tx:"+hash)).String() }
func idAccount(addr string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("acct:"+addr)).String()
}
func idAssetXRP() string { return uuid.NewSHA1(uuid.NameSpaceURL, []byte("asset:XRP::")).String() }
func idAssetIOU(currency, issuer string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("asset:IOU:"+currency+":"+issuer)).String()
}

// Human-readable symbol resolution for IOU currencies
func decodeHexCurrency(cur string) (string, bool) {
	if len(cur) != 40 { // 20 bytes hex
		return "", false
	}
	b, err := hex.DecodeString(cur)
	if err != nil {
		return "", false
	}
	// trim trailing zero bytes
	end := len(b)
	for end > 0 && b[end-1] == 0x00 {
		end--
	}
	b = b[:end]
	if len(b) == 0 {
		return "", false
	}
	// ensure printable ASCII
	for _, c := range b {
		if c < 32 || c > 126 {
			return "", false
		}
	}
	s := string(b)
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false
	}
	return s, true
}

func symbolFromCurrencyMap(m map[string]interface{}) string {
	if v, ok := m["_currency"].(string); ok && v != "" {
		return v
	}
	cur, _ := m["currency"].(string)
	if s, ok := decodeHexCurrency(cur); ok {
		return s
	}
	return cur
}

func normCurrency(c string) string { return strings.ToUpper(c) }

// Serial consumer (based on callback function) for low volume message streams
func RunConsumer(conn *kafka.Reader, callback func(m kafka.Message)) {
	ctx := context.Background()
	for {
		m, err := conn.FetchMessage(ctx)
		if err != nil {
			break
		}
		callback(m)

		if err := conn.CommitMessages(ctx, m); err != nil {
			logger.Log.Error().Err(err).Msg("Failed to commit kafka message")
		}
	}
}

// Bulk message consumer (based on channel) for high volume message streams
func RunBulkConsumer(conn *kafka.Reader, callback func(<-chan kafka.Message)) {
	ctx := context.Background()
	ch := make(chan kafka.Message)
	go callback(ch)

	for {
		m, err := conn.FetchMessage(ctx)
		if err != nil {
			break
		}

		ch <- m

		if err := conn.CommitMessages(ctx, m); err != nil {
			logger.Log.Error().Err(err).Msg("Failed to commit kafka message")
		}
	}
}

// Run all consumers
func RunConsumers() {
	// Only run the transaction transformer; drop other noisy consumers
	go RunBulkConsumer(connections.KafkaReaderTransaction, func(ch <-chan kafka.Message) {
		ctx := context.Background()
		for {
			m := <-ch
			var tx map[string]interface{}
			if err := json.Unmarshal(m.Value, &tx); err != nil {
				logger.Log.Error().Err(err).Msg("Transaction json.Unmarshal error")
				continue
			}
			// Filter by allowed transaction types at code level
			if tt, ok := tx["TransactionType"].(string); ok {
				if tt != "Payment" {
					// skip unwanted transaction types
					continue
				}
			} else {
				// unknown or missing type; skip
				continue
			}
			modified, err := indexer.ModifyTransaction(tx)
			if err != nil {
				logger.Log.Error().Err(err).Msg("Error fixing transaction object")
				continue
			}
			b, err := json.Marshal(modified)
			if err != nil {
				logger.Log.Error().Err(err).Msg("Transaction json.Marshal error")
				continue
			}

			// Emit CH rows: transactions, assets, money_flows
			var base map[string]interface{} = modified
			hash, _ := base["hash"].(string)
			ledgerIndex, _ := base["ledger_index"].(float64)
			closeTime, _ := base["date"].(float64)
			account, _ := base["Account"].(string)
			destination, _ := base["Destination"].(string)
			result := ""
			if meta, ok := base["meta"].(map[string]interface{}); ok {
				if r, ok := meta["TransactionResult"].(string); ok {
					if r == "tesSUCCESS" {
						result = r
					} else {
						continue
					}
				}
			}
			feeDrops := uint64(0)
			if fee, ok := base["Fee"].(float64); ok {
				feeDrops = uint64(fee)
			} else if feeStr, ok := base["Fee"].(string); ok {
				if v, err := strconv.ParseUint(feeStr, 10, 64); err == nil {
					feeDrops = v
				}
			}

			// transactions row (final JSON for CH MV)
			txId := idTx(hash)
			accountId := idAccount(account)
			destId := idAccount(destination)
			const rippleToUnix int64 = 946684800
			closeTimeUnix := int64(closeTime) + rippleToUnix
			txRow := models.CHTransactionRow{
				TxID:          txId,
				Hash:          hash,
				LedgerIndex:   uint32(ledgerIndex),
				CloseTimeUnix: closeTimeUnix,
				TxType:        "Payment",
				AccountID:     accountId,
				DestinationID: destId,
				Result:        result,
				FeeDrops:      feeDrops,
				RawJSON:       string(b),
			}
			if row, err := json.Marshal(txRow); err == nil {
				_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHTransactions(), Key: []byte(hash), Value: row})
			}

			// ensure XRP asset exists in assets table (emit once per process)
			if _, loaded := emittedAssets.LoadOrStore("XRP", true); !loaded {
				xrpRow := models.CHAssetRow{
					AssetID:   idAssetXRP(),
					AssetType: "XRP",
					Currency:  "XRP",
					IssuerID:  uuid.Nil.String(),
					Symbol:    "XRP",
				}
				if row, err := json.Marshal(xrpRow); err == nil {
					_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAssets(), Key: []byte("XRP"), Value: row})
				}
			}

			// accounts rows: emit deduped final JSON rows for CH accounts
			if account != "" {
				aid := idAccount(account)
				if _, loaded := emittedAssets.LoadOrStore("acc:"+account, true); !loaded {
					ar := models.CHAccountRow{AccountID: aid, Address: account}
					if row, err := json.Marshal(ar); err == nil {
						_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAccounts(), Key: []byte(account), Value: row})
					}
				}
			}
			if destination != "" {
				did := idAccount(destination)
				if _, loaded := emittedAssets.LoadOrStore("acc:"+destination, true); !loaded {
					dr := models.CHAccountRow{AccountID: did, Address: destination}
					if row, err := json.Marshal(dr); err == nil {
						_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAccounts(), Key: []byte(destination), Value: row})
					}
				}
			}

			// assets rows from tx fields (preferred canonical sources)
			issuersByCurrency := make(map[string]string)
			// 1) Amount
			if amt, ok := base["Amount"].(map[string]interface{}); ok {
				cur, _ := amt["currency"].(string)
				cur = normCurrency(cur)
				iss, _ := amt["issuer"].(string)
				if cur != "" && iss != "" {
					issuersByCurrency[cur] = iss
					assetKey := "IOU:" + cur + ":" + iss
					if _, loaded := emittedAssets.LoadOrStore(assetKey, true); !loaded {
						sym := symbolFromCurrencyMap(amt)
						issuerUUID := idAccount(iss)
						assetRow := models.CHAssetRow{AssetID: idAssetIOU(cur, iss), AssetType: "IOU", Currency: cur, IssuerID: issuerUUID, Symbol: sym}
						if row, err := json.Marshal(assetRow); err == nil {
							_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAssets(), Key: []byte(assetKey), Value: row})
						}
						if _, loaded2 := emittedAssets.LoadOrStore("acc:"+iss, true); !loaded2 {
							ar := models.CHAccountRow{AccountID: issuerUUID, Address: iss}
							if row2, err2 := json.Marshal(ar); err2 == nil {
								_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAccounts(), Key: []byte(iss), Value: row2})
							}
						}
					}
				}
			}
			// 2) SendMax
			if sm, ok := base["SendMax"].(map[string]interface{}); ok {
				cur, _ := sm["currency"].(string)
				cur = normCurrency(cur)
				iss, _ := sm["issuer"].(string)
				if cur != "" && iss != "" {
					if _, ok := issuersByCurrency[cur]; !ok {
						issuersByCurrency[cur] = iss
					}
					assetKey := "IOU:" + cur + ":" + iss
					if _, loaded := emittedAssets.LoadOrStore(assetKey, true); !loaded {
						sym := symbolFromCurrencyMap(sm)
						issuerUUID := idAccount(iss)
						assetRow := models.CHAssetRow{AssetID: idAssetIOU(cur, iss), AssetType: "IOU", Currency: cur, IssuerID: issuerUUID, Symbol: sym}
						if row, err := json.Marshal(assetRow); err == nil {
							_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAssets(), Key: []byte(assetKey), Value: row})
						}
						if _, loaded2 := emittedAssets.LoadOrStore("acc:"+iss, true); !loaded2 {
							ar := models.CHAccountRow{AccountID: issuerUUID, Address: iss}
							if row2, err2 := json.Marshal(ar); err2 == nil {
								_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAccounts(), Key: []byte(iss), Value: row2})
							}
						}
					}
				}
			}
			// 3) meta.delivered_amount
			if meta, ok := base["meta"].(map[string]interface{}); ok {
				if da, ok := meta["delivered_amount"].(map[string]interface{}); ok {
					cur, _ := da["currency"].(string)
					cur = normCurrency(cur)
					iss, _ := da["issuer"].(string)
					if cur != "" && iss != "" {
						issuersByCurrency[cur] = iss
						assetKey := "IOU:" + cur + ":" + iss
						if _, loaded := emittedAssets.LoadOrStore(assetKey, true); !loaded {
							sym := symbolFromCurrencyMap(da)
							issuerUUID := idAccount(iss)
							assetRow := models.CHAssetRow{AssetID: idAssetIOU(cur, iss), AssetType: "IOU", Currency: cur, IssuerID: issuerUUID, Symbol: sym}
							if row, err := json.Marshal(assetRow); err == nil {
								_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAssets(), Key: []byte(assetKey), Value: row})
							}
							if _, loaded2 := emittedAssets.LoadOrStore("acc:"+iss, true); !loaded2 {
								ar := models.CHAccountRow{AccountID: issuerUUID, Address: iss}
								if row2, err2 := json.Marshal(ar); err2 == nil {
									_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAccounts(), Key: []byte(iss), Value: row2})
								}
							}
						}
					}
				}
			}

			// money_flow rows derived from AffectedNodes:
			// - transfer: AccountRoot Balance deltas (XRP)
			// - dexOffer/swap: Offer execution inferred from Balance deltas across two assets
			if meta, ok := base["meta"].(map[string]interface{}); ok {
				if nodes, ok := meta["AffectedNodes"].([]interface{}); ok {
					// First pass: collect balance deltas (XRP and IOU) per account+asset
					type assetKey struct{ currency, issuer string }
					type offerPair struct {
						owner string
						gets  assetKey
						pays  assetKey
						quote decimal.Decimal // pays per gets
					}
					offersByOwner := make(map[string][]offerPair)
					balances := make(map[string]map[assetKey]decimal.Decimal)

					CHMoneyFlowRows := make([]models.CHMoneyFlowRow, 0)

					if account == destination {
						amount_1_prev := decimal.NewFromFloat(0)
						amount_1_final := decimal.NewFromFloat(0)
		
						amount_2_prev := decimal.NewFromFloat(0)
						amount_2_final := decimal.NewFromFloat(0)
		
						delta_1 := decimal.NewFromFloat(0)
						delta_1_currency := ""
						delta_1_issuer := ""
						delta_2 := decimal.NewFromFloat(0)
						delta_2_currency := ""
						delta_2_issuer := ""
		
						delta_1_filled := false
						for _, n := range nodes {
							node, _ := n.(map[string]interface{})

							var modified1 map[string]interface{}
							var ok1 bool
							if modified1, ok1 = node["ModifiedNode"].(map[string]interface{}); !ok1 {
								modified1, ok1 = node["DeletedNode"].(map[string]interface{})
							}
							if ok1 {
								if ledgerEntryType, ok := modified1["LedgerEntryType"].(string); ok {
									if ledgerEntryType == "Offer" {
										if finalFields, ok := modified1["FinalFields"].(map[string]interface{}); ok {
											if previousFields, ok := modified1["PreviousFields"].(map[string]interface{}); ok {
												if _, ok := finalFields["Account"].(string); ok {
													var prevTakerGetsValue, finalTakerGetsValue decimal.Decimal
													var prevTakerGetsCurrency string
													var prevTakerGetsIssuer string
		
													if prevTakerGets, exists := previousFields["TakerGets"]; exists {
														switch v := prevTakerGets.(type) {
														case string:
															dec, err := decimal.NewFromString(v)
															if err == nil {
																prevTakerGetsValue = dec.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
															}
															prevTakerGetsCurrency = "XRP"
														case map[string]interface{}:
															if currency, ok := v["currency"].(string); ok {
																prevTakerGetsCurrency = currency
															}
															if issuer, ok := v["issuer"].(string); ok {
																prevTakerGetsIssuer = issuer
															}
															if value, ok := v["value"].(string); ok {
																dec, err := decimal.NewFromString(value)
																if err == nil {
																	prevTakerGetsValue = dec
																}
															}
														}
													}
		
													if finalTakerGets, exists := finalFields["TakerGets"]; exists {
														switch v := finalTakerGets.(type) {
														case string:
															dec, err := decimal.NewFromString(v)
															if err == nil {
																finalTakerGetsValue = dec.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
															}
														case map[string]interface{}:
															if value, ok := v["value"].(string); ok {
																dec, err := decimal.NewFromString(value)
																if err == nil {
																	finalTakerGetsValue = dec
																}
															}
														}
													}
		
													var prevTakerPaysValue, finalTakerPaysValue decimal.Decimal
													var prevTakerPaysCurrency string
													var prevTakerPaysIssuer string
		
													if prevTakerPays, exists := previousFields["TakerPays"]; exists {
														switch v := prevTakerPays.(type) {
														case string:
															dec, err := decimal.NewFromString(v)
															if err == nil {
																prevTakerPaysValue = dec.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
															}
															prevTakerPaysCurrency = "XRP"
														case map[string]interface{}:
															if currency, ok := v["currency"].(string); ok {
																prevTakerPaysCurrency = currency
															}
															if issuer, ok := v["issuer"].(string); ok {
																prevTakerPaysIssuer = issuer
															}
															if value, ok := v["value"].(string); ok {
																dec, err := decimal.NewFromString(value)
																if err == nil {
																	prevTakerPaysValue = dec
																}
															}
														}
													}
		
													if finalTakerPays, exists := finalFields["TakerPays"]; exists {
														switch v := finalTakerPays.(type) {
														case string:
															dec, err := decimal.NewFromString(v)
															if err == nil {
																finalTakerPaysValue = dec.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
															}
														case map[string]interface{}:
															if value, ok := v["value"].(string); ok {
																dec, err := decimal.NewFromString(value)
																if err == nil {
																	finalTakerPaysValue = dec
																}
															}
														}
													}
		
													takerGetsDelta := prevTakerGetsValue.Sub(finalTakerGetsValue)
													takerPaysDelta := prevTakerPaysValue.Sub(finalTakerPaysValue)
		
													rate := takerGetsDelta.Div(takerPaysDelta)
		
													fmt.Printf("TakerGets Delta (владелец отдал): %s %s\n", takerGetsDelta.Neg().String(), prevTakerGetsCurrency)
													if prevTakerGetsIssuer != "" {
														fmt.Printf("TakerGets Issuer: %s\n", prevTakerGetsIssuer)
													}
		
													fmt.Printf("TakerPays Delta (владелец получил): %s %s\n", takerPaysDelta, prevTakerPaysCurrency)
													if prevTakerPaysIssuer != "" {
														fmt.Printf("TakerPays Issuer: %s\n", prevTakerPaysIssuer)
													}
		
													fmt.Printf("Rate: %s\n", rate.String())
		
													fmt.Println("=====================================")
												}
											}
										}
									}
		
								}
							}

							if modified, ok := node["ModifiedNode"].(map[string]interface{}); ok {
								if final_fields, ok := modified["FinalFields"].(map[string]interface{}); ok {
									if node_account, ok := final_fields["Account"].(string); ok {
										if account == node_account {
											amount_account_final := decimal.NewFromFloat(0)
											amount_account_prev := decimal.NewFromFloat(0)
											currency_account := ""
											issuer_account := ""
											if balance, ok := final_fields["Balance"].(map[string]interface{}); ok {
												if vs, ok := balance["value"].(string); ok {
													amount_account_final, _ = decimal.NewFromString(vs)
												}
												if currency, ok := balance["currency"].(string); ok {
													currency_account = currency
												}
												if issuer, ok := balance["issuer"].(string); ok {
													issuer_account = issuer
												}
											} else if balanceStr, ok := final_fields["Balance"].(string); ok {
												if v, err := decimal.NewFromString(balanceStr); err == nil {
													amount_account_final = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
												}
												if currency, ok := balance["currency"].(string); ok {
													currency_account = currency
												} else {
													currency_account = "XRP"
												}
												if issuer, ok := balance["issuer"].(string); ok {
													issuer_account = issuer
												} else {
													issuer_account = ""
												}
											}
		
											if previous_fields, ok := modified["PreviousFields"].(map[string]interface{}); ok {
												if balance, ok := previous_fields["Balance"].(map[string]interface{}); ok {
													if vs, ok := balance["value"].(string); ok {
														amount_account_prev, _ = decimal.NewFromString(vs)
													}
													if currency, ok := balance["currency"].(string); ok {
														currency_account = currency
													} else {
														currency_account = "XRP"
													}
													if issuer, ok := balance["issuer"].(string); ok {
														issuer_account = issuer
													} else {
														issuer_account = ""
													}
												} else if balanceStr, ok := previous_fields["Balance"].(string); ok {
													if v, err := decimal.NewFromString(balanceStr); err == nil {
														amount_account_prev = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
													}
													if currency, ok := balance["currency"].(string); ok {
														currency_account = currency
													} else {
														currency_account = "XRP"
													}
													if issuer, ok := balance["issuer"].(string); ok {
														issuer_account = issuer
													} else {
														issuer_account = ""
													}
												}
											}
		
											feeXRP := decimal.NewFromInt(int64(feeDrops)).Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
											diff := amount_account_final.Sub(amount_account_prev)

											// fmt.Println("=====================================")
											// fmt.Printf("diff: %s\n", diff.String())
											// fmt.Printf("feeXRP: %s\n", feeXRP.String())
											// fmt.Printf("diff.Add(feeXRP).IsZero(): %s\n", diff.Add(feeXRP).String())
											// fmt.Printf("diff.Add(feeXRP).IsZero(): %t\n", diff.Add(feeXRP).IsZero())
											// fmt.Println("=====================================")

											if !diff.Add(feeXRP).IsZero() {
												if !delta_1_filled {
													delta_1 = diff.Add(feeXRP)
													delta_1_currency = currency_account
													delta_1_issuer = issuer_account
													delta_1_filled = true
												} else {
													delta_2 = diff.Add(feeXRP)
													delta_2_currency = currency_account
													delta_2_issuer = issuer_account
												}
											}
										}
									}
		
									if high_limit, ok := final_fields["HighLimit"].(map[string]interface{}); ok {
										if account == high_limit["issuer"].(string) {
											currency_high_limit := ""
											issuer_high_limit := ""
											if balance, ok := final_fields["Balance"].(map[string]interface{}); ok {
												if vs, ok := balance["value"].(string); ok {
													if !delta_1_filled {
														amount_1_final, _ = decimal.NewFromString(vs)
													} else {
														amount_2_final, _ = decimal.NewFromString(vs)
													}
													if currency, ok := high_limit["currency"].(string); ok {
														currency_high_limit = currency
													} else {
														currency_high_limit = "XRP"
													}
													if issuer, ok := high_limit["issuer"].(string); ok {
														issuer_high_limit = issuer
													} else {
														issuer_high_limit = ""
													}
												}
											} else if balanceStr, ok := final_fields["Balance"].(string); ok {
												if v, err := decimal.NewFromString(balanceStr); err == nil {
													if !delta_1_filled {
														amount_1_final = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
													} else {
														amount_2_final = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
													}
													if currency, ok := high_limit["currency"].(string); ok {
														currency_high_limit = currency
													} else {
														currency_high_limit = "XRP"
													}
													if issuer, ok := high_limit["issuer"].(string); ok {
														issuer_high_limit = issuer
													} else {
														issuer_high_limit = ""
													}
												}
											}
		
											if previous_fields, ok := modified["PreviousFields"].(map[string]interface{}); ok {
												if balance, ok := previous_fields["Balance"].(map[string]interface{}); ok {
													if vs, ok := balance["value"].(string); ok {
														if !delta_1_filled {
															amount_1_prev, _ = decimal.NewFromString(vs)
														} else {
															amount_2_prev, _ = decimal.NewFromString(vs)
														}
														if currency, ok := balance["currency"].(string); ok {
															currency_high_limit = currency
														} else {
															currency_high_limit = "XRP"
														}
														if issuer, ok := balance["issuer"].(string); ok {
															issuer_high_limit = issuer
														} else {
															issuer_high_limit = ""
														}
													}
												} else if balanceStr, ok := previous_fields["Balance"].(string); ok {
													if v, err := decimal.NewFromString(balanceStr); err == nil {
														if !delta_1_filled {
															amount_1_prev = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
														} else {
															amount_2_prev = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
														}
														if currency, ok := balance["currency"].(string); ok {
															currency_high_limit = currency
														} else {
															currency_high_limit = "XRP"
														}
														if issuer, ok := balance["issuer"].(string); ok {
															issuer_high_limit = issuer
														} else {
															issuer_high_limit = ""
														}
													}
												}
											}
		
											if !delta_1_filled {
												if amount_1_final.IsNegative() {
													amount_1_final = amount_1_final.Neg()
												}
		
												if amount_1_prev.IsNegative() {
													amount_1_prev = amount_1_prev.Neg()
												}
												delta_1 = amount_1_final.Sub(amount_1_prev)
												delta_1_currency = currency_high_limit
												delta_1_issuer = issuer_high_limit
												delta_1_filled = true
											} else {
												if amount_2_final.IsNegative() {
													amount_2_final = amount_2_final.Neg()
												}
		
												if amount_2_prev.IsNegative() {
													amount_2_prev = amount_2_prev.Neg()
												}
												delta_2 = amount_2_final.Sub(amount_2_prev)
												delta_2_currency = currency_high_limit
												delta_2_issuer = issuer_high_limit
											}
										}
									}
		
									if low_limit, ok := final_fields["LowLimit"].(map[string]interface{}); ok {
										if account == low_limit["issuer"].(string) {
											currency_low_limit := ""
											issuer_low_limit := ""
											if balance, ok := final_fields["Balance"].(map[string]interface{}); ok {
												if vs, ok := balance["value"].(string); ok {
													if !delta_1_filled {
														amount_1_final, _ = decimal.NewFromString(vs)
													} else {
														amount_2_final, _ = decimal.NewFromString(vs)
													}
													if currency, ok := low_limit["currency"].(string); ok {
														currency_low_limit = currency
													} else {
														currency_low_limit = "XRP"
													}
													if issuer, ok := low_limit["issuer"].(string); ok {
														issuer_low_limit = issuer
													} else {
														issuer_low_limit = ""
													}
												}
											} else if balanceStr, ok := final_fields["Balance"].(string); ok {
												if v, err := decimal.NewFromString(balanceStr); err == nil {
													if !delta_1_filled {
														amount_1_final = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
													} else {
														amount_2_final = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
													}
													if currency, ok := low_limit["currency"].(string); ok {
														currency_low_limit = currency
													} else {
														currency_low_limit = "XRP"
													}
													if issuer, ok := low_limit["issuer"].(string); ok {
														issuer_low_limit = issuer
													} else {
														issuer_low_limit = ""
													}
												}
											}
		
											if previous_fields, ok := modified["PreviousFields"].(map[string]interface{}); ok {
												if balance, ok := previous_fields["Balance"].(map[string]interface{}); ok {
													if vs, ok := balance["value"].(string); ok {
														if !delta_1_filled {
															amount_1_prev, _ = decimal.NewFromString(vs)
														} else {
															amount_2_prev, _ = decimal.NewFromString(vs)
														}
														if currency, ok := balance["currency"].(string); ok {
															currency_low_limit = currency
														} else {
															currency_low_limit = "XRP"
														}
														if issuer, ok := balance["issuer"].(string); ok {
															issuer_low_limit = issuer
														} else {
															issuer_low_limit = ""
														}
													}
												} else if balanceStr, ok := previous_fields["Balance"].(string); ok {
													if v, err := decimal.NewFromString(balanceStr); err == nil {
														if !delta_1_filled {
															amount_1_prev = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
														} else {
															amount_2_prev = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
														}
														if currency, ok := balance["currency"].(string); ok {
															currency_low_limit = currency
														} else {
															currency_low_limit = "XRP"
														}
														if issuer, ok := balance["issuer"].(string); ok {
															issuer_low_limit = issuer
														} else {
															issuer_low_limit = ""
														}
													}
												}
											}
		
											if !delta_1_filled {
												if amount_1_final.IsNegative() {
													amount_1_final = amount_1_final.Neg()
												}
		
												if amount_1_prev.IsNegative() {
													amount_1_prev = amount_1_prev.Neg()
												}
												delta_1 = amount_1_final.Sub(amount_1_prev)
												delta_1_currency = currency_low_limit
												delta_1_issuer = issuer_low_limit
												delta_1_filled = true
											} else {
												if amount_2_final.IsNegative() {
													amount_2_final = amount_2_final.Neg()
												}
		
												if amount_2_prev.IsNegative() {
													amount_2_prev = amount_2_prev.Neg()
												}
												delta_2 = amount_2_final.Sub(amount_2_prev)
												delta_2_currency = currency_low_limit
												delta_2_issuer = issuer_low_limit
											}
										}
									}
		
								}
							}




							var fields map[string]interface{}
							var obj map[string]interface{}
							if created, ok := node["CreatedNode"].(map[string]interface{}); ok {
								obj = created
								fields, _ = created["NewFields"].(map[string]interface{})
							} else if modified, ok := node["ModifiedNode"].(map[string]interface{}); ok {
								obj = modified
								fields, _ = modified["FinalFields"].(map[string]interface{})
							} else if deleted, ok := node["DeletedNode"].(map[string]interface{}); ok {
								obj = deleted
								fields, _ = deleted["FinalFields"].(map[string]interface{})
							}
							if fields == nil {
								continue
							}
							ledgerType, _ := obj["LedgerEntryType"].(string)
							switch ledgerType {
							case "AccountRoot":
								addr, _ := fields["Account"].(string)
								if addr == "" {
									continue
								}
								var prevBalance, finalBalance int64
								if modified, ok := node["ModifiedNode"].(map[string]interface{}); ok {
									if pf, ok := modified["PreviousFields"].(map[string]interface{}); ok {
										if pb, ok := pf["Balance"].(string); ok {
											if v, err := strconv.ParseInt(pb, 10, 64); err == nil {
												prevBalance = v
											}
										}
									}
									if ff, ok := modified["FinalFields"].(map[string]interface{}); ok {
										if fb, ok := ff["Balance"].(string); ok {
											if v, err := strconv.ParseInt(fb, 10, 64); err == nil {
												finalBalance = v
											}
										}
									}
								}
								if prevBalance != 0 || finalBalance != 0 {
									delta := decimal.NewFromInt(finalBalance - prevBalance).Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
									if !delta.IsZero() {
										if _, ok := balances[addr]; !ok {
											balances[addr] = make(map[assetKey]decimal.Decimal)
										}
										k := assetKey{currency: "XRP", issuer: ""}
										balances[addr][k] = balances[addr][k].Add(delta)
									}
								}
							case "RippleState":
								// IOU trustline balance delta
								// Extract account, currency, issuer and delta from PreviousFields/FinalFields.Balance.value
								high, _ := fields["HighLimit"].(map[string]interface{})
								low, _ := fields["LowLimit"].(map[string]interface{})
								bal, _ := fields["Balance"].(map[string]interface{})
								currency, _ := bal["currency"].(string)
								currency = normCurrency(currency)
								issuerHigh, _ := high["issuer"].(string)
								issuerLow, _ := low["issuer"].(string)
								// Determine which side increased/decreased by comparing PreviousFields/FinalFields
								var prevV, finalV decimal.Decimal
								if modified, ok := node["ModifiedNode"].(map[string]interface{}); ok {
									if pf, ok := modified["PreviousFields"].(map[string]interface{}); ok {
										if pb, ok := pf["Balance"].(map[string]interface{}); ok {
											if vs, ok := pb["value"].(string); ok {
												prevV, _ = decimal.NewFromString(vs)
											}
										}
									}
									if ff, ok := modified["FinalFields"].(map[string]interface{}); ok {
										if fb, ok := ff["Balance"].(map[string]interface{}); ok {
											if vs, ok := fb["value"].(string); ok {
												finalV, _ = decimal.NewFromString(vs)
											}
										}
									}
								}
								if prevV.Equal(finalV) {
									continue
								}
								// RippleState.balance is from low->high perspective
								// delta > 0 => transfer from high to low (low receives)
								abs := finalV.Sub(prevV)
								recv := issuerLow
								send := issuerHigh
								if abs.IsNegative() {
									// delta < 0 => transfer low -> high
									abs = abs.Neg()
									recv = issuerHigh
									send = issuerLow
								}
								issuer := issuersByCurrency[currency]
								if _, ok := balances[recv]; !ok {
									balances[recv] = make(map[assetKey]decimal.Decimal)
								}
								if _, ok := balances[send]; !ok {
									balances[send] = make(map[assetKey]decimal.Decimal)
								}
								k := assetKey{currency: currency, issuer: issuer}
								balances[recv][k] = balances[recv][k].Add(abs) // receiver +abs
								balances[send][k] = balances[send][k].Sub(abs) // sender -abs

								// Do not emit assets from RippleState to avoid issuer ambiguity
							case "Offer":
								// Collect offer asset pairs and approximate execution quote from deltas
								owner, _ := fields["Account"].(string)
								if owner == "" {
									break
								}
								// Parse helper
								parseAmount := func(v interface{}) (assetKey, decimal.Decimal) {
									if m, ok := v.(map[string]interface{}); ok {
										cur, _ := m["currency"].(string)
										cur = normCurrency(cur)
										iss, _ := m["issuer"].(string)
										vs, _ := m["value"].(string)
										val, _ := decimal.NewFromString(vs)
										return assetKey{currency: cur, issuer: iss}, val
									}
									if s, ok := v.(string); ok {
										// XRP in drops
										iv, _ := decimal.NewFromString(s)
										x := iv.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
										return assetKey{currency: "XRP", issuer: ""}, x
									}
									return assetKey{}, decimal.Zero
								}
								getPrevFinal := func(obj map[string]interface{}) (prevGets, finGets assetKey, prevG, finG decimal.Decimal, prevPays, finPays assetKey, prevP, finP decimal.Decimal) {
									var pf, ff map[string]interface{}
									if modified, ok := obj["ModifiedNode"].(map[string]interface{}); ok {
										pf, _ = modified["PreviousFields"].(map[string]interface{})
										ff, _ = modified["FinalFields"].(map[string]interface{})
									} else if deleted, ok := obj["DeletedNode"].(map[string]interface{}); ok {
										pf, _ = deleted["PreviousFields"].(map[string]interface{})
										ff, _ = deleted["FinalFields"].(map[string]interface{})
									} else if created, ok := obj["CreatedNode"].(map[string]interface{}); ok {
										ff, _ = created["NewFields"].(map[string]interface{})
									}
									if pf != nil {
										pgAK, pg := parseAmount(pf["TakerGets"]) // previous gets
										ppAK, pp := parseAmount(pf["TakerPays"]) // previous pays
										prevGets, prevG = pgAK, pg
										prevPays, prevP = ppAK, pp
									}
									if ff != nil {
										fgAK, fg := parseAmount(ff["TakerGets"]) // final gets
										fpAK, fp := parseAmount(ff["TakerPays"]) // final pays
										finGets, finG = fgAK, fg
										finPays, finP = fpAK, fp
									}
									return
								}
								prevGetsAK, finGetsAK, prevGetsAmt, finGetsAmt, prevPaysAK, finPaysAK, prevPaysAmt, finPaysAmt := getPrevFinal(node)
								// Prefer asset keys from FinalFields when available, else PreviousFields
								getsAK := finGetsAK
								paysAK := finPaysAK
								if getsAK.currency == "" {
									getsAK = prevGetsAK
								}
								if paysAK.currency == "" {
									paysAK = prevPaysAK
								}
								// Compute executed amounts (previous - final)
								execGets := prevGetsAmt.Sub(finGetsAmt)
								execPays := prevPaysAmt.Sub(finPaysAmt)
								if execGets.IsNegative() {
									execGets = execGets.Neg()
								}
								if execPays.IsNegative() {
									execPays = execPays.Neg()
								}
								quote := decimal.Zero
								if !execGets.IsZero() {
									quote = execPays.Div(execGets)
								}
								// Record offer pair only if we have both sides
								if getsAK.currency != "" && (paysAK.currency != "" || (paysAK.currency == "XRP" && paysAK.issuer == "")) {
									offersByOwner[owner] = append(offersByOwner[owner], offerPair{owner: owner, gets: getsAK, pays: paysAK, quote: quote})
								}
							}
						}

						from_amount := decimal.NewFromFloat(0)
						to_amount := decimal.NewFromFloat(0)
		
						if amount_1_final.LessThan(amount_1_prev) {
							from_amount = delta_1
							to_amount = delta_2
						} else {
							from_amount = delta_2
							to_amount = delta_1
						}
		
						rate := from_amount.Neg().Div(to_amount)
						fmt.Printf("=== ДЕЛЬТА ДЛЯ ИНИЦИАТОРА СВАПА ===\n")
						fmt.Printf("Account: %s\n", account)
						fmt.Printf("from_amount: %s\n", from_amount.Truncate(15).String())
						fmt.Printf("from_amount_currency: %s\n", delta_1_currency)
						fmt.Printf("from_amount_issuer: %s\n", delta_1_issuer)
						fmt.Printf("to_amount: %s\n", to_amount.Truncate(15).String())
						fmt.Printf("to_amount_currency: %s\n", delta_2_currency)
						fmt.Printf("to_amount_issuer: %s\n", delta_2_issuer)
						fmt.Printf("Rate: %s\n", rate.Truncate(15).String())
						fmt.Println("=====================================")
					} else {
						fmt.Printf("=== Трансфер ===\n")
		
						from_id := idAccount(account)
						to_id := idAccount(destination)

						from_asset_id := ""
						to_asset_id := ""
		
						fmt.Printf("from_id: %s\n", from_id)
						fmt.Printf("to_id: %s\n", to_id)
		
						amount := decimal.NewFromFloat(0)
						amount_issuer := ""
						amount_currency := ""

						if amountField, ok := base["Amount"].(map[string]interface{}); ok {
							if vs, ok := amountField["value"].(string); ok {
								if _, ok := amountField["native"].(bool); ok {
									if v, err := decimal.NewFromString(vs); err == nil {
										amount = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
									}
								} else {
									amount, _ = decimal.NewFromString(vs)
								}
							}
							if issuer, ok := amountField["issuer"].(string); ok {
								amount_issuer = issuer
							} else {
								amount_issuer = ""
							}
							if currency, ok := amountField["currency"].(string); ok {
								amount_currency = currency
							} else {
								amount_currency = ""
							}
						}
						
						if amount_currency == "XRP" {
							from_asset_id = idAssetXRP()
						} else {
							from_asset_id = idAssetIOU(normCurrency(amount_currency), amount_issuer)
						}

						if amount_currency == "XRP" {
							to_asset_id = idAssetXRP()
						} else {
							to_asset_id = idAssetIOU(normCurrency(amount_currency), amount_issuer)
						}

						CHMoneyFlowRows = append(CHMoneyFlowRows, models.CHMoneyFlowRow{
							TxID: txId,
							FromID: from_id,
							ToID: to_id,
							FromAssetID: from_asset_id,
							ToAssetID: to_asset_id,
							FromAmount: amount.Neg().String(),
							ToAmount: amount.String(),
							Quote: "1",
							Kind: "transfer",
						})

						// fmt.Printf("CHMoneyFlowRows: %+v\n", CHMoneyFlowRows)
					}

					fmt.Println("==============HASH==============")
					fmt.Println(hash)
					fmt.Println("=====================================")

					for _, row := range CHMoneyFlowRows {
						if row, err := json.Marshal(row); err == nil {
							_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHMoneyFlows(), Key: []byte(hash), Value: row})
						}
					}
				}
			}
		}
	})
}
