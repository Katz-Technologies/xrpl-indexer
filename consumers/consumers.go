package consumers

import (
	"context"
	"encoding/hex"
	"encoding/json"
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

func decodeHexCurrency(cur string) (string, bool) {
	if len(cur) != 40 {
		return "", false
	}
	b, err := hex.DecodeString(cur)
	if err != nil {
		return "", false
	}
	end := len(b)
	for end > 0 && b[end-1] == 0x00 {
		end--
	}
	b = b[:end]
	if len(b) == 0 {
		return "", false
	}
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

func RunConsumers() {
	go RunBulkConsumer(connections.KafkaReaderTransaction, func(ch <-chan kafka.Message) {
		ctx := context.Background()
		for {
			m := <-ch
			var tx map[string]interface{}
			if err := json.Unmarshal(m.Value, &tx); err != nil {
				logger.Log.Error().Err(err).Msg("Transaction json.Unmarshal error")
				continue
			}
			if tt, ok := tx["TransactionType"].(string); ok {
				if tt != "Payment" {
					continue
				}
			} else {
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
			switch v := base["Fee"].(type) {
			case float64:
				feeDrops = uint64(v)
			case int64:
				feeDrops = uint64(v)
			case int:
				feeDrops = uint64(v)
			case string:
				if parsed, err := strconv.ParseUint(v, 10, 64); err == nil {
					feeDrops = parsed
				}
			case json.Number:
				if parsed, err := v.Int64(); err == nil {
					feeDrops = uint64(parsed)
				}
			}

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

			issuersByCurrency := make(map[string]string)
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

			if meta, ok := base["meta"].(map[string]interface{}); ok {
				if nodes, ok := meta["AffectedNodes"].([]interface{}); ok {
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
												if accountAddress, ok := finalFields["Account"].(string); ok {
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
		
													from_id := idAccount(accountAddress)
													to_id := idAccount(accountAddress)
							
													from_asset_id := ""
													to_asset_id := ""
		
													takerGetsDelta.Div(takerPaysDelta)
		
													if prevTakerGetsCurrency == "XRP" {
														from_asset_id = idAssetXRP()
													} else {
														from_asset_id = idAssetIOU(normCurrency(prevTakerGetsCurrency), prevTakerGetsIssuer)
													}
													
													if prevTakerPaysCurrency == "XRP" {
														to_asset_id = idAssetXRP()
													} else {
														to_asset_id = idAssetIOU(normCurrency(prevTakerPaysCurrency), prevTakerPaysIssuer)
													}
		
													CHMoneyFlowRows = append(CHMoneyFlowRows, models.CHMoneyFlowRow{
														TxID: txId,
														FromID: from_id,
														ToID: to_id,
														FromAssetID: from_asset_id,
														ToAssetID: to_asset_id,
														FromAmount: takerGetsDelta.Neg().String(),
														ToAmount: takerPaysDelta.String(),
														Quote: rate.String(),
														Kind: "dexOffer",
													})
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
		
									currency_low_limit := ""
									issuer_low_limit := ""
		
									if high_limit, ok := final_fields["HighLimit"].(map[string]interface{}); ok {
										if account == high_limit["issuer"].(string) {
											if low_limit, ok := final_fields["LowLimit"].(map[string]interface{}); ok {
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
		
											if balance, ok := final_fields["Balance"].(map[string]interface{}); ok {
												if vs, ok := balance["value"].(string); ok {
													if !delta_1_filled {
														amount_1_final, _ = decimal.NewFromString(vs)
													} else {
														amount_2_final, _ = decimal.NewFromString(vs)
													}
												}
											} else if balanceStr, ok := final_fields["Balance"].(string); ok {
												if v, err := decimal.NewFromString(balanceStr); err == nil {
													if !delta_1_filled {
														amount_1_final = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
													} else {
														amount_2_final = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
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
													}
												} else if balanceStr, ok := previous_fields["Balance"].(string); ok {
													if v, err := decimal.NewFromString(balanceStr); err == nil {
														if !delta_1_filled {
															amount_1_prev = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
														} else {
															amount_2_prev = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
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
		
									if low_limit, ok := final_fields["LowLimit"].(map[string]interface{}); ok {
										if account == low_limit["issuer"].(string) {
											currency_high_limit := ""
											issuer_high_limit := ""
		
											if high_limit, ok := final_fields["HighLimit"].(map[string]interface{}); ok {
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
											if balance, ok := final_fields["Balance"].(map[string]interface{}); ok {
												if vs, ok := balance["value"].(string); ok {
													if !delta_1_filled {
														amount_1_final, _ = decimal.NewFromString(vs)
													} else {
														amount_2_final, _ = decimal.NewFromString(vs)
													}
													
												}
											} else if balanceStr, ok := final_fields["Balance"].(string); ok {
												if v, err := decimal.NewFromString(balanceStr); err == nil {
													if !delta_1_filled {
														amount_1_final = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
													} else {
														amount_2_final = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
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
														
													}
												} else if balanceStr, ok := previous_fields["Balance"].(string); ok {
													if v, err := decimal.NewFromString(balanceStr); err == nil {
														if !delta_1_filled {
															amount_1_prev = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
														} else {
															amount_2_prev = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
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
		
								}
							}
						}
		
						from_amount := decimal.NewFromFloat(0)
						from_currency := ""
						from_issuer := ""
						to_amount := decimal.NewFromFloat(0)
						to_currency := ""
						to_issuer := ""
		
						if delta_1.LessThan(decimal.NewFromFloat(0)) {
							from_amount = delta_1
							from_currency = delta_1_currency
							from_issuer = delta_1_issuer
							to_amount = delta_2
							to_currency = delta_2_currency
							to_issuer = delta_2_issuer
						} else {
							from_amount = delta_2
							from_currency = delta_2_currency
							from_issuer = delta_2_issuer
							to_amount = delta_1
							to_currency = delta_1_currency
							to_issuer = delta_1_issuer
						}
		
						rate := from_amount.Neg().Div(to_amount)
						from_id := idAccount(account)
						to_id := idAccount(destination)
		
						to_asset_id := ""
						from_asset_id := ""
		
						if to_currency == "XRP" {
							to_asset_id = idAssetXRP()
						} else {
							to_asset_id = idAssetIOU(normCurrency(to_currency), to_issuer)
						}
		
						if from_currency == "XRP" {
							from_asset_id = idAssetXRP()
						} else {
							from_asset_id = idAssetIOU(normCurrency(from_currency), from_issuer)
						}

						CHMoneyFlowRows = append(CHMoneyFlowRows, models.CHMoneyFlowRow{
							TxID: txId,
							FromID: from_id,
							ToID: to_id,
							FromAssetID: from_asset_id,
							ToAssetID: to_asset_id,
							FromAmount: from_amount.String(),
							ToAmount: to_amount.String(),
							Quote: rate.String(),
							Kind: "swap",
						})
					} else {
						from_id := idAccount(account)
						to_id := idAccount(destination)

						from_asset_id := ""
						to_asset_id := ""
		
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
					}

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
