package consumers

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
	"github.com/xrpscan/platform/config"
	"github.com/xrpscan/platform/connections"
	"github.com/xrpscan/platform/indexer"
	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/platform/models"
)

func idTx(hash string) string { return uuid.NewSHA1(uuid.NameSpaceURL, []byte("tx:"+hash)).String() }
func idAccount(addr string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("acct:"+addr)).String()
}
func idAssetXRP() string { return "7ab3a23b-28ba-5fb4-aac1-b3546017b182" }
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

// generateVersion creates a timestamp-based version for ReplacingMergeTree deduplication
func generateVersion() uint64 {
	return uint64(time.Now().UnixNano())
}

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
			inLedgerIndex := float64(0)
			if meta, ok := base["meta"].(map[string]interface{}); ok {
				if r, ok := meta["TransactionResult"].(string); ok {
					if r == "tesSUCCESS" {
						result = r
					} else {
						continue
					}
				}
				if ti, ok := meta["TransactionIndex"].(float64); ok {
					inLedgerIndex = ti
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
				InLedgerIndex: uint32(inLedgerIndex),
				Version:       generateVersion(),
			}
			if row, err := json.Marshal(txRow); err == nil {
				_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHTransactions(), Key: []byte(hash), Value: row})
			}

			if account != "" {
				aid := idAccount(account)
				ar := models.CHAccountRow{AccountID: aid, Address: account, Version: generateVersion()}
				if row, err := json.Marshal(ar); err == nil {
					_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAccounts(), Key: []byte(account), Value: row})
				}
			}
			if destination != "" {
				did := idAccount(destination)
				dr := models.CHAccountRow{AccountID: did, Address: destination, Version: generateVersion()}
				if row, err := json.Marshal(dr); err == nil {
					_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAccounts(), Key: []byte(destination), Value: row})
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
					sym := symbolFromCurrencyMap(amt)
					issuerUUID := idAccount(iss)
					assetRow := models.CHAssetRow{AssetID: idAssetIOU(cur, iss), AssetType: "IOU", Currency: cur, IssuerID: issuerUUID, Symbol: sym, Version: generateVersion()}
					if row, err := json.Marshal(assetRow); err == nil {
						_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAssets(), Key: []byte(assetKey), Value: row})
					}
					ar := models.CHAccountRow{AccountID: issuerUUID, Address: iss, Version: generateVersion()}
					if row2, err2 := json.Marshal(ar); err2 == nil {
						_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAccounts(), Key: []byte(iss), Value: row2})
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
					sym := symbolFromCurrencyMap(sm)
					issuerUUID := idAccount(iss)
					assetRow := models.CHAssetRow{AssetID: idAssetIOU(cur, iss), AssetType: "IOU", Currency: cur, IssuerID: issuerUUID, Symbol: sym, Version: generateVersion()}
					if row, err := json.Marshal(assetRow); err == nil {
						_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAssets(), Key: []byte(assetKey), Value: row})
					}
					ar := models.CHAccountRow{AccountID: issuerUUID, Address: iss, Version: generateVersion()}
					if row2, err2 := json.Marshal(ar); err2 == nil {
						_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAccounts(), Key: []byte(iss), Value: row2})
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
						sym := symbolFromCurrencyMap(da)
						issuerUUID := idAccount(iss)
						assetRow := models.CHAssetRow{AssetID: idAssetIOU(cur, iss), AssetType: "IOU", Currency: cur, IssuerID: issuerUUID, Symbol: sym, Version: generateVersion()}
						if row, err := json.Marshal(assetRow); err == nil {
							_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAssets(), Key: []byte(assetKey), Value: row})
						}
						ar := models.CHAccountRow{AccountID: issuerUUID, Address: iss, Version: generateVersion()}
						if row2, err2 := json.Marshal(ar); err2 == nil {
							_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAccounts(), Key: []byte(iss), Value: row2})
						}
					}
				}
			}

			if meta, ok := base["meta"].(map[string]interface{}); ok {
				if nodes, ok := meta["AffectedNodes"].([]interface{}); ok {
					CHMoneyFlowRows := make([]models.CHMoneyFlowRow, 0)
					CHAccountsRows := make([]models.CHAccountRow, 0)

					if account == destination {
						amount_1_prev := decimal.Zero
						amount_1_final := decimal.Zero

						amount_2_prev := decimal.Zero
						amount_2_final := decimal.Zero

						init_1 := decimal.Zero
						delta_1 := decimal.Zero
						delta_1_currency := ""
						delta_1_issuer := ""
						init_2 := decimal.Zero
						delta_2 := decimal.Zero
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
													var prevTakerGetsValue, finalTakerGetsValue decimal.Decimal = decimal.Zero, decimal.Zero
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

													var prevTakerPaysValue, finalTakerPaysValue decimal.Decimal = decimal.Zero, decimal.Zero
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

													var rate decimal.Decimal = decimal.Zero
													if takerPaysDelta.IsZero() {
														rate = decimal.Zero
													} else {
														rate = takerGetsDelta.Div(takerPaysDelta)
													}

													from_id := idAccount(accountAddress)
													to_id := idAccount(accountAddress)

													from_asset_id := ""
													to_asset_id := ""

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

													init_from_amount := decimal.Zero
													init_to_amount := decimal.Zero

													for _, n2 := range nodes {
														node2, _ := n2.(map[string]interface{})
														if modifiedNode, ok := node2["ModifiedNode"].(map[string]interface{}); ok {
															if ledgerEntryType, ok := modifiedNode["LedgerEntryType"].(string); ok {
																if prevTakerGetsCurrency == "XRP" && ledgerEntryType == "AccountRoot" {
																	if finalFields, ok := modifiedNode["FinalFields"].(map[string]interface{}); ok {
																		if acc, ok := finalFields["Account"].(string); ok && acc == accountAddress {
																			if previousFields, ok := modifiedNode["PreviousFields"].(map[string]interface{}); ok {
																				if balanceStr, ok := previousFields["Balance"].(string); ok {
																					if v, err := decimal.NewFromString(balanceStr); err == nil {
																						init_from_amount = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
																					}
																				}
																			}
																		}
																	}
																} else if prevTakerGetsCurrency != "XRP" && ledgerEntryType == "RippleState" {
																	if finalFields, ok := modifiedNode["FinalFields"].(map[string]interface{}); ok {
																		matchesFromAsset := false
																		if lowLimit, ok := finalFields["LowLimit"].(map[string]interface{}); ok {
																			if lowIssuer, ok := lowLimit["issuer"].(string); ok {
																				if lowCur, ok := lowLimit["currency"].(string); ok {
																					if lowIssuer == accountAddress && normCurrency(lowCur) == normCurrency(prevTakerGetsCurrency) {
																						if highLimit, ok := finalFields["HighLimit"].(map[string]interface{}); ok {
																							if highIssuer, ok := highLimit["issuer"].(string); ok {
																								if highIssuer == prevTakerGetsIssuer {
																									matchesFromAsset = true
																								}
																							}
																						}
																					}
																				}
																			}
																		}
																		if !matchesFromAsset {
																			if highLimit, ok := finalFields["HighLimit"].(map[string]interface{}); ok {
																				if highIssuer, ok := highLimit["issuer"].(string); ok {
																					if highCur, ok := highLimit["currency"].(string); ok {
																						if highIssuer == accountAddress && normCurrency(highCur) == normCurrency(prevTakerGetsCurrency) {
																							if lowLimit, ok := finalFields["LowLimit"].(map[string]interface{}); ok {
																								if lowIssuer, ok := lowLimit["issuer"].(string); ok {
																									if lowIssuer == prevTakerGetsIssuer {
																										matchesFromAsset = true
																									}
																								}
																							}
																						}
																					}
																				}
																			}
																		}
																		if matchesFromAsset {
																			if previousFields, ok := modifiedNode["PreviousFields"].(map[string]interface{}); ok {
																				if balance, ok := previousFields["Balance"].(map[string]interface{}); ok {
																					if vs, ok := balance["value"].(string); ok {
																						if v, err := decimal.NewFromString(vs); err == nil {
																							init_from_amount = v.Abs()
																						}
																					}
																				}
																			}
																		}
																	}
																}

																if prevTakerPaysCurrency == "XRP" && ledgerEntryType == "AccountRoot" {
																	if finalFields, ok := modifiedNode["FinalFields"].(map[string]interface{}); ok {
																		if acc, ok := finalFields["Account"].(string); ok && acc == accountAddress {
																			if previousFields, ok := modifiedNode["PreviousFields"].(map[string]interface{}); ok {
																				if balanceStr, ok := previousFields["Balance"].(string); ok {
																					if v, err := decimal.NewFromString(balanceStr); err == nil {
																						init_to_amount = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
																					}
																				}
																			}
																		}
																	}
																} else if prevTakerPaysCurrency != "XRP" && ledgerEntryType == "RippleState" {
																	if finalFields, ok := modifiedNode["FinalFields"].(map[string]interface{}); ok {
																		matchesToAsset := false
																		if lowLimit, ok := finalFields["LowLimit"].(map[string]interface{}); ok {
																			if lowIssuer, ok := lowLimit["issuer"].(string); ok {
																				if lowCur, ok := lowLimit["currency"].(string); ok {
																					if lowIssuer == accountAddress && normCurrency(lowCur) == normCurrency(prevTakerPaysCurrency) {
																						if highLimit, ok := finalFields["HighLimit"].(map[string]interface{}); ok {
																							if highIssuer, ok := highLimit["issuer"].(string); ok {
																								if highIssuer == prevTakerPaysIssuer {
																									matchesToAsset = true
																								}
																							}
																						}
																					}
																				}
																			}
																		}
																		if !matchesToAsset {
																			if highLimit, ok := finalFields["HighLimit"].(map[string]interface{}); ok {
																				if highIssuer, ok := highLimit["issuer"].(string); ok {
																					if highCur, ok := highLimit["currency"].(string); ok {
																						if highIssuer == accountAddress && normCurrency(highCur) == normCurrency(prevTakerPaysCurrency) {
																							if lowLimit, ok := finalFields["LowLimit"].(map[string]interface{}); ok {
																								if lowIssuer, ok := lowLimit["issuer"].(string); ok {
																									if lowIssuer == prevTakerPaysIssuer {
																										matchesToAsset = true
																									}
																								}
																							}
																						}
																					}
																				}
																			}
																		}
																		if matchesToAsset {
																			if previousFields, ok := modifiedNode["PreviousFields"].(map[string]interface{}); ok {
																				if balance, ok := previousFields["Balance"].(map[string]interface{}); ok {
																					if vs, ok := balance["value"].(string); ok {
																						if v, err := decimal.NewFromString(vs); err == nil {
																							init_to_amount = v.Abs()
																						}
																					}
																				}
																			}
																		}
																	}
																}
															}
														}
													}

													CHMoneyFlowRows = append(CHMoneyFlowRows, models.CHMoneyFlowRow{
														TxID:           txId,
														FromID:         from_id,
														ToID:           to_id,
														FromAssetID:    from_asset_id,
														ToAssetID:      to_asset_id,
														FromAmount:     takerGetsDelta.Neg().String(),
														ToAmount:       takerPaysDelta.String(),
														InitFromAmount: init_from_amount.String(),
														InitToAmount:   init_to_amount.String(),
														Quote:          rate.String(),
														Kind:           "dexOffer",
														Version:        generateVersion(),
													})

													CHAccountsRows = append(CHAccountsRows, models.CHAccountRow{AccountID: from_id, Address: accountAddress, Version: generateVersion()})
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
											amount_account_final := decimal.Zero
											amount_account_prev := decimal.Zero
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
													init_1 = amount_account_prev
													delta_1_currency = currency_account
													delta_1_issuer = issuer_account
													delta_1_filled = true
												} else {
													delta_2 = diff.Add(feeXRP)
													init_2 = amount_account_prev
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
												init_1 = amount_1_prev
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
												init_2 = amount_2_prev
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
												init_1 = amount_1_prev
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
												init_2 = amount_2_prev
												delta_2_currency = currency_high_limit
												delta_2_issuer = issuer_high_limit
											}
										}
									}

								}
							}
						}

						from_amount := decimal.Zero
						init_from_amount := decimal.Zero
						from_currency := ""
						from_issuer := ""
						to_amount := decimal.Zero
						init_to_amount := decimal.Zero
						to_currency := ""
						to_issuer := ""

						if delta_1.LessThan(decimal.Zero) {
							from_amount = delta_1
							init_from_amount = init_1
							from_currency = delta_1_currency
							from_issuer = delta_1_issuer
							to_amount = delta_2
							init_to_amount = init_2
							to_currency = delta_2_currency
							to_issuer = delta_2_issuer
						} else {
							from_amount = delta_2
							init_from_amount = init_2
							from_currency = delta_2_currency
							from_issuer = delta_2_issuer
							to_amount = delta_1
							init_to_amount = init_1
							to_currency = delta_1_currency
							to_issuer = delta_1_issuer
						}

						var rate decimal.Decimal = decimal.Zero
						if to_amount.IsZero() {
							rate = decimal.Zero
						} else {
							rate = from_amount.Neg().Div(to_amount)
						}
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
							TxID:           txId,
							FromID:         from_id,
							ToID:           to_id,
							FromAssetID:    from_asset_id,
							ToAssetID:      to_asset_id,
							FromAmount:     from_amount.String(),
							ToAmount:       to_amount.String(),
							InitFromAmount: init_from_amount.String(),
							InitToAmount:   init_to_amount.String(),
							Quote:          rate.String(),
							Kind:           "swap",
							Version:        generateVersion(),
						})
						CHAccountsRows = append(CHAccountsRows, models.CHAccountRow{AccountID: from_id, Address: account, Version: generateVersion()})
					} else {
						from_id := idAccount(account)
						to_id := idAccount(destination)

						from_asset_id := ""
						to_asset_id := ""

						amount := decimal.Zero
						amount_issuer := ""
						amount_currency := ""

            if amountField, ok := meta["delivered_amount"].(map[string]interface{}); ok {
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
                amount_currency = "XRP"
              }
            } else {
              if amountField, ok := meta["delivered_amount"].(string); ok {
                amount, _ = decimal.NewFromString(amountField)
                amount_issuer = ""
                amount_currency = "XRP"
              }
            }

						init_amount := decimal.Zero

						for _, n := range nodes {
							node, _ := n.(map[string]interface{})

							if modifiedNodes, ok := node["ModifiedNode"].(map[string]interface{}); ok {
								if ledgerEntryType, ok := modifiedNodes["LedgerEntryType"].(string); ok {
									if amount_currency == "XRP" {
										if ledgerEntryType == "AccountRoot" {
											if finalField, ok := modifiedNodes["FinalFields"].(map[string]interface{}); ok {
												if finalFieldAccount, ok := finalField["Account"].(string); ok {
													if finalFieldAccount == account {
														if previousField, ok := modifiedNodes["PreviousFields"].(map[string]interface{}); ok {
															if balanceStr, ok := previousField["Balance"].(string); ok {
																if v, err := decimal.NewFromString(balanceStr); err == nil {
																	init_amount = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
																}
															}
														}
													}
												}
											}
										}
									} else {
										if ledgerEntryType == "RippleState" {
											if finalField, ok := modifiedNodes["FinalFields"].(map[string]interface{}); ok {
												if lowLimit, ok := finalField["LowLimit"].(map[string]interface{}); ok {
													if lowLimitAccount, ok := lowLimit["issuer"].(string); ok {
														if highLimit, ok := finalField["HighLimit"].(map[string]interface{}); ok {
															if highLimitAccount, ok := highLimit["issuer"].(string); ok {
																if lowLimitAccount == account || highLimitAccount == account {
																	if previousField, ok := modifiedNodes["PreviousFields"].(map[string]interface{}); ok {
																		if balance, ok := previousField["Balance"].(map[string]interface{}); ok {
																			if vs, ok := balance["value"].(string); ok {
																				init_amount, _ = decimal.NewFromString(vs)
																			}
																		}
																	}
																}
															}
														}
													}
												}
											}
										}
									}
								}
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
							TxID:           txId,
							FromID:         from_id,
							ToID:           to_id,
							FromAssetID:    from_asset_id,
							ToAssetID:      to_asset_id,
							FromAmount:     amount.Neg().String(),
							ToAmount:       amount.String(),
							InitFromAmount: init_amount.Abs().String(),
							InitToAmount:   init_amount.Abs().String(),
							Quote:          "1",
							Kind:           "transfer",
							Version:        generateVersion(),
						})
						CHAccountsRows = append(CHAccountsRows, models.CHAccountRow{AccountID: from_id, Address: account, Version: generateVersion()})
						CHAccountsRows = append(CHAccountsRows, models.CHAccountRow{AccountID: to_id, Address: destination, Version: generateVersion()})
					}

					for _, row := range CHMoneyFlowRows {
						if row, err := json.Marshal(row); err == nil {
							_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHMoneyFlows(), Key: []byte(hash), Value: row})
						}
					}

					for _, row := range CHAccountsRows {
						address := row.Address
						if row, err := json.Marshal(row); err == nil {
							_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAccounts(), Key: []byte(address), Value: row})
						}
					}
				}
			}
		}
	})
}
