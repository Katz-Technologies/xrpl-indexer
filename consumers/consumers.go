package consumers

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
	"github.com/xrpscan/platform/config"
	"github.com/xrpscan/platform/connections"
	"github.com/xrpscan/platform/indexer"
	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/platform/models"
)

// decodeHexCurrency конвертирует HEX валюту в читаемый символ
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

// isHexCurrency проверяет, является ли строка HEX валютой
func isHexCurrency(cur string) bool {
	// HEX валюта обычно 40 символов (20 байт в hex)
	if len(cur) != 40 {
		return false
	}
	// Проверяем, что все символы - это hex цифры
	for _, c := range cur {
		if !((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

func normCurrency(c string) string { return strings.ToUpper(c) }

// currencyToSymbol конвертирует HEX валюту в символ, иначе возвращает как есть
func currencyToSymbol(cur string) string {
	if isHexCurrency(cur) {
		if symbol, ok := decodeHexCurrency(cur); ok {
			return symbol
		}
		return "" // Если не удалось декодировать
	}
	return strings.ToUpper(cur) // Обычная валюта как есть
}

// fixIssuerForXRP заменяет пустую строку issuer на "XRP" для XRP валюты
func fixIssuerForXRP(currency, issuer string) string {
	if currency == "XRP" && issuer == "" {
		return "XRP"
	}
	return issuer
}

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

			var base map[string]interface{} = modified
			hash, _ := base["hash"].(string)
			ledgerIndex, _ := base["ledger_index"].(float64)
			closeTime, _ := base["date"].(float64)
			account, _ := base["Account"].(string)
			destination, _ := base["Destination"].(string)
			// Убрано result - больше не используется в новой схеме
			inLedgerIndex := float64(0)
			if meta, ok := base["meta"].(map[string]interface{}); ok {
				if r, ok := meta["TransactionResult"].(string); ok {
					if r == "tesSUCCESS" {
						// Убрано присваивание result - больше не используется
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

			// Убрано создание txId, accountId, destId - больше не нужны для новой схемы
			const rippleToUnix int64 = 946684800
			closeTimeUnix := int64(closeTime) + rippleToUnix
			// Убрано создание CHTransactionRow - данные теперь идут в money_flow

			issuersByCurrency := make(map[string]string)
			if amt, ok := base["Amount"].(map[string]interface{}); ok {
				cur, _ := amt["currency"].(string)
				cur = normCurrency(cur)
				iss, _ := amt["issuer"].(string)
				if cur != "" && iss != "" {
					issuersByCurrency[cur] = iss
					// Убрано создание CHAssetRow - таблицы assets больше нет
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
					// Убрано создание CHAssetRow - таблицы assets больше нет
				}
			}
			if meta, ok := base["meta"].(map[string]interface{}); ok {
				if da, ok := meta["delivered_amount"].(map[string]interface{}); ok {
					cur, _ := da["currency"].(string)
					cur = normCurrency(cur)
					iss, _ := da["issuer"].(string)
					if cur != "" && iss != "" {
						issuersByCurrency[cur] = iss
						// Убрано создание CHAssetRow - таблицы assets больше нет
					}
				}
			}

			if meta, ok := base["meta"].(map[string]interface{}); ok {
				if nodes, ok := meta["AffectedNodes"].([]interface{}); ok {
					CHMoneyFlowRows := make([]models.CHMoneyFlowRow, 0)
					// Убрано CHAccountsRows - таблицы accounts больше нет

					// Определяем валюты из SendMax и delivered_amount для использования в основной логике
					expected_from_currency := ""
					expected_from_issuer := ""
					expected_to_currency := ""
					expected_to_issuer := ""

					// Определяем from_currency и from_issuer из SendMax
					if sm, ok := base["SendMax"].(map[string]interface{}); ok {
						if cur, ok := sm["currency"].(string); ok {
							expected_from_currency = currencyToSymbol(cur)
						}
						if iss, ok := sm["issuer"].(string); ok {
							expected_from_issuer = fixIssuerForXRP(expected_from_currency, iss)
						}
					}

					// Определяем to_currency и to_issuer из delivered_amount или Amount
					if da, ok := meta["delivered_amount"].(map[string]interface{}); ok {
						if cur, ok := da["currency"].(string); ok {
							expected_to_currency = currencyToSymbol(cur)
						}
						if iss, ok := da["issuer"].(string); ok {
							expected_to_issuer = fixIssuerForXRP(expected_to_currency, iss)
						}
					} else if amountField, ok := base["Amount"].(map[string]interface{}); ok {
						if cur, ok := amountField["currency"].(string); ok {
							expected_to_currency = currencyToSymbol(cur)
						}
						if iss, ok := amountField["issuer"].(string); ok {
							expected_to_issuer = fixIssuerForXRP(expected_to_currency, iss)
						}
					} else if amountStr, ok := base["Amount"].(string); ok {
						// Amount - это строка (XRP в drops)
						expected_to_currency = "XRP"
						expected_to_issuer = "XRP"
						// amountStr используется для определения суммы, но здесь мы только определяем валюты
						_ = amountStr // подавляем предупреждение о неиспользуемой переменной
					}

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

													// Убрано создание from_id, to_id - больше не нужны для новой схемы

													// Убрано создание from_asset_id, to_asset_id - больше не нужны для новой схемы

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
														TxHash:            hash,
														LedgerIndex:       uint32(ledgerIndex),
														InLedgerIndex:     uint32(inLedgerIndex),
														CloseTimeUnix:     closeTimeUnix,
														FeeDrops:          feeDrops,
														FromAddress:       accountAddress,
														ToAddress:         accountAddress,
														FromCurrency:      currencyToSymbol(prevTakerGetsCurrency),
														FromIssuerAddress: fixIssuerForXRP(currencyToSymbol(prevTakerGetsCurrency), prevTakerGetsIssuer),
														ToCurrency:        currencyToSymbol(prevTakerPaysCurrency),
														ToIssuerAddress:   fixIssuerForXRP(currencyToSymbol(prevTakerPaysCurrency), prevTakerPaysIssuer),
														FromAmount:        takerGetsDelta.Neg().String(),
														ToAmount:          takerPaysDelta.String(),
														InitFromAmount:    init_from_amount.String(),
														InitToAmount:      init_to_amount.String(),
														Quote:             rate.String(),
														Kind:              "dexOffer",
														Version:           generateVersion(),
													})

													// Убрано добавление в CHAccountsRows - таблицы accounts больше нет
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
													issuer_account = "XRP" // Для XRP устанавливаем "XRP" вместо пустой строки
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
													issuer_low_limit = "XRP" // Для XRP устанавливаем "XRP" вместо пустой строки
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
													issuer_high_limit = "XRP" // Для XRP устанавливаем "XRP" вместо пустой строки
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
						// Убрано создание from_id, to_id - больше не нужны для новой схемы

						// Убрано создание from_asset_id, to_asset_id - больше не нужны для новой схемы

						// Используем ожидаемые валюты если delta валюты пустые
						if from_currency == "" && expected_from_currency != "" {
							from_currency = expected_from_currency
							from_issuer = expected_from_issuer
						}
						if to_currency == "" && expected_to_currency != "" {
							to_currency = expected_to_currency
							to_issuer = expected_to_issuer
						}

						CHMoneyFlowRows = append(CHMoneyFlowRows, models.CHMoneyFlowRow{
							TxHash:            hash,
							LedgerIndex:       uint32(ledgerIndex),
							InLedgerIndex:     uint32(inLedgerIndex),
							CloseTimeUnix:     closeTimeUnix,
							FeeDrops:          feeDrops,
							FromAddress:       account,
							ToAddress:         destination,
							FromCurrency:      currencyToSymbol(from_currency),
							FromIssuerAddress: fixIssuerForXRP(currencyToSymbol(from_currency), from_issuer),
							ToCurrency:        currencyToSymbol(to_currency),
							ToIssuerAddress:   fixIssuerForXRP(currencyToSymbol(to_currency), to_issuer),
							FromAmount:        from_amount.String(),
							ToAmount:          to_amount.String(),
							InitFromAmount:    init_from_amount.String(),
							InitToAmount:      init_to_amount.String(),
							Quote:             rate.String(),
							Kind:              "swap",
							Version:           generateVersion(),
						})
						// Убрано добавление в CHAccountsRows - таблицы accounts больше нет
					} else {
						transfer_to_issuer := false

						if amount, ok := base["Amount"].(map[string]interface{}); ok {
							if issuer, ok := amount["issuer"].(string); ok {
								if issuer == destination {
									transfer_to_issuer = true
								}
							}
						}

						if transfer_to_issuer {
							// --- Инициализация переменных ---
							account_init_from_amount := decimal.Zero
							account_from_amount := decimal.Zero
						
							amm_address := ""
							amm_init_from_amount := decimal.Zero
							amm_init_to_amount := decimal.Zero
							amm_from_amount := decimal.Zero
							amm_to_amount := decimal.Zero
						
							// --- Определяем валюту burn-транзакции ---
							burn_currency := ""
							burn_issuer := destination
							burn_amount := decimal.Zero
						
							if amount, ok := base["Amount"].(map[string]interface{}); ok {
								if curr, ok := amount["currency"].(string); ok {
									burn_currency = curr
								}
								if val, ok := amount["value"].(string); ok {
									if v, err := decimal.NewFromString(val); err == nil {
										burn_amount = v
									}
								}
							}
						
							// --- Проверяем, участвует ли AMM ---
							isAmmInvolved := false
						
							for _, n := range nodes {
								node, _ := n.(map[string]interface{})
								if modifiedNodes, ok := node["ModifiedNode"].(map[string]interface{}); ok {
									if ledgerEntryType, ok := modifiedNodes["LedgerEntryType"].(string); ok {
										if ledgerEntryType == "AccountRoot" {
											if finalField, ok := modifiedNodes["FinalFields"].(map[string]interface{}); ok {
												if finalFieldAccount, ok := finalField["Account"].(string); ok {
													// AMM — это не отправитель и не получатель (issuer)
													if finalFieldAccount != account && finalFieldAccount != destination {
														isAmmInvolved = true
														amm_address = finalFieldAccount
													}
												}
											}
										}
									}
								}
							}
							
							isBurnEnd := false
							// ===================================================================
							// === CASE 1: Manual burn (отправитель → эмитент напрямую) ==========
							// ===================================================================
							if !isAmmInvolved {
								CHMoneyFlowRows = append(CHMoneyFlowRows, models.CHMoneyFlowRow{
									TxHash:            hash,
									LedgerIndex:       uint32(ledgerIndex),
									InLedgerIndex:     uint32(inLedgerIndex),
									CloseTimeUnix:     closeTimeUnix,
									FeeDrops:          feeDrops,
									FromAddress:       account,
									ToAddress:         "",
									FromCurrency:      currencyToSymbol(burn_currency),
									FromIssuerAddress: burn_issuer,
									ToCurrency:        currencyToSymbol(burn_currency),
									ToIssuerAddress:   burn_issuer,
									FromAmount:        burn_amount.Abs().Neg().String(),
									ToAmount:          decimal.Zero.String(),
									InitFromAmount:    burn_amount.Abs().String(),
									InitToAmount:      decimal.Zero.String(),
									Quote:             "1",
									Kind:              "transfer",
									Version:           generateVersion(),
								})
								isBurnEnd = true
							}
						
							// ===================================================================
							// === CASE 2: Burn через AMM (path payment к issuer) ================
							// ===================================================================
							// Собираем данные по узлам, как раньше
							if !isBurnEnd {
								for _, n := range nodes {
									node, _ := n.(map[string]interface{})
									if modifiedNodes, ok := node["ModifiedNode"].(map[string]interface{}); ok {
										if ledgerEntryType, ok := modifiedNodes["LedgerEntryType"].(string); ok {
											switch ledgerEntryType {
											case "AccountRoot":
												if finalField, ok := modifiedNodes["FinalFields"].(map[string]interface{}); ok {
													if finalFieldAccount, ok := finalField["Account"].(string); ok {
														// === Изменение баланса отправителя (XRP) ===
														if finalFieldAccount == account {
															if previousField, ok := modifiedNodes["PreviousFields"].(map[string]interface{}); ok {
																if balanceStr, ok := previousField["Balance"].(string); ok {
																	if v, err := decimal.NewFromString(balanceStr); err == nil {
																		account_init_from_amount = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
																		if finalFieldBalance, ok := finalField["Balance"].(string); ok {
																			if v, err := decimal.NewFromString(finalFieldBalance); err == nil {
																				final_field_amount := v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
																				account_from_amount = final_field_amount.Sub(account_init_from_amount)
																			}
																		}
																	}
																}
															}
														} else { // === Баланс AMM ===
															if previousField, ok := modifiedNodes["PreviousFields"].(map[string]interface{}); ok {
																if balanceStr, ok := previousField["Balance"].(string); ok {
																	if v, err := decimal.NewFromString(balanceStr); err == nil {
																		amm_init_from_amount = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
																	}
																	if finalFieldBalance, ok := finalField["Balance"].(string); ok {
																		if v, err := decimal.NewFromString(finalFieldBalance); err == nil {
																			final_field_amount := v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
																			amm_from_amount = final_field_amount.Sub(amm_init_from_amount)
																		}
																	}
																}
															}
														}
													}
												}
											case "RippleState":
												if finalField, ok := modifiedNodes["FinalFields"].(map[string]interface{}); ok {
													if lowLimit, ok := finalField["LowLimit"].(map[string]interface{}); ok {
														if lowLimitAccount, ok := lowLimit["issuer"].(string); ok {
															if highLimit, ok := finalField["HighLimit"].(map[string]interface{}); ok {
																if highLimitAccount, ok := highLimit["issuer"].(string); ok {
																	if lowLimitAccount == destination || highLimitAccount == destination {
																		if previousField, ok := modifiedNodes["PreviousFields"].(map[string]interface{}); ok {
																			if balance, ok := previousField["Balance"].(map[string]interface{}); ok {
																				if vs, ok := balance["value"].(string); ok {
																					amm_init_to_amount, _ = decimal.NewFromString(vs)
																				}
																				if finalFieldBalance, ok := finalField["Balance"].(map[string]interface{}); ok {
																					if vs, ok := finalFieldBalance["value"].(string); ok {
																						final_field_amount, _ := decimal.NewFromString(vs)
																						amm_to_amount = final_field_amount.Sub(amm_init_to_amount)
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
							
								// === Запись 1: Отправитель → AMM (XRP) ===
								CHMoneyFlowRows = append(CHMoneyFlowRows, models.CHMoneyFlowRow{
									TxHash:            hash,
									LedgerIndex:       uint32(ledgerIndex),
									InLedgerIndex:     uint32(inLedgerIndex),
									CloseTimeUnix:     closeTimeUnix,
									FeeDrops:          feeDrops,
									FromAddress:       account,
									ToAddress:         amm_address,
									FromCurrency:      "XRP",
									FromIssuerAddress: "XRP",
									ToCurrency:        "XRP",
									ToIssuerAddress:   "XRP",
									FromAmount:        account_from_amount.Abs().Neg().String(),
									ToAmount:          amm_to_amount.Abs().String(),
									InitFromAmount:    account_init_from_amount.Abs().String(),
									InitToAmount:      amm_init_to_amount.Abs().String(),
									Quote:             "1",
									Kind:              "transfer",
									Version:           generateVersion(),
								})
							
								// === Запись 2: AMM → issuer (burn IOU) ===
								CHMoneyFlowRows = append(CHMoneyFlowRows, models.CHMoneyFlowRow{
									TxHash:            hash,
									LedgerIndex:       uint32(ledgerIndex),
									InLedgerIndex:     uint32(inLedgerIndex),
									CloseTimeUnix:     closeTimeUnix,
									FeeDrops:          feeDrops,
									FromAddress:       amm_address,
									ToAddress:         "",
									FromCurrency:      currencyToSymbol(burn_currency),
									FromIssuerAddress: burn_issuer,
									ToCurrency:        currencyToSymbol(burn_currency),
									ToIssuerAddress:   burn_issuer,
									FromAmount:        amm_from_amount.Abs().Neg().String(),
									ToAmount:          decimal.Zero.String(),
									InitFromAmount:    amm_init_from_amount.Abs().String(),
									InitToAmount:      decimal.Zero.String(),
									Quote:             "1",
									Kind:              "transfer",
									Version:           generateVersion(),
								})
							}
						} else {

							// Убрано создание from_id, to_id - больше не нужны для новой схемы

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
									amount_issuer = "XRP"
								}
								if currency, ok := amountField["currency"].(string); ok {
									amount_currency = currency
								} else {
									amount_currency = "XRP"
								}
							} else {
								if amountField, ok := meta["delivered_amount"].(string); ok {
									amount, _ = decimal.NewFromString(amountField)
									amount_issuer = "XRP"
									amount_currency = "XRP"
								}
							}

							init_from_amount := decimal.Zero
							init_to_amount := decimal.Zero

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
																		init_from_amount = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
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
																					init_from_amount, _ = decimal.NewFromString(vs)
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

							for _, n := range nodes {
								node, _ := n.(map[string]interface{})

								if modifiedNodes, ok := node["ModifiedNode"].(map[string]interface{}); ok {
									if ledgerEntryType, ok := modifiedNodes["LedgerEntryType"].(string); ok {
										if amount_currency == "XRP" {
											if ledgerEntryType == "AccountRoot" {
												if finalField, ok := modifiedNodes["FinalFields"].(map[string]interface{}); ok {
													if finalFieldAccount, ok := finalField["Account"].(string); ok {
														if finalFieldAccount == destination {
															if previousField, ok := modifiedNodes["PreviousFields"].(map[string]interface{}); ok {
																if balanceStr, ok := previousField["Balance"].(string); ok {
																	if v, err := decimal.NewFromString(balanceStr); err == nil {
																		init_to_amount = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
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
																	if lowLimitAccount == destination || highLimitAccount == destination {
																		if previousField, ok := modifiedNodes["PreviousFields"].(map[string]interface{}); ok {
																			if balance, ok := previousField["Balance"].(map[string]interface{}); ok {
																				if vs, ok := balance["value"].(string); ok {
																					init_to_amount, _ = decimal.NewFromString(vs)
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

							// Используем ожидаемые валюты если amount_currency пустая
							if amount_currency == "" {
								if expected_from_currency != "" {
									amount_currency = expected_from_currency
									amount_issuer = expected_from_issuer
								} else if expected_to_currency != "" {
									amount_currency = expected_to_currency
									amount_issuer = expected_to_issuer
								}
							}

							CHMoneyFlowRows = append(CHMoneyFlowRows, models.CHMoneyFlowRow{
								TxHash:            hash,
								LedgerIndex:       uint32(ledgerIndex),
								InLedgerIndex:     uint32(inLedgerIndex),
								CloseTimeUnix:     closeTimeUnix,
								FeeDrops:          feeDrops,
								FromAddress:       account,
								ToAddress:         destination,
								FromCurrency:      currencyToSymbol(amount_currency),
								FromIssuerAddress: fixIssuerForXRP(currencyToSymbol(amount_currency), amount_issuer),
								ToCurrency:        currencyToSymbol(amount_currency),
								ToIssuerAddress:   fixIssuerForXRP(currencyToSymbol(amount_currency), amount_issuer),
								FromAmount:        amount.Neg().String(),
								ToAmount:          amount.String(),
								InitFromAmount:    init_from_amount.Abs().String(),
								InitToAmount:      init_to_amount.Abs().String(),
								Quote:             "1",
								Kind:              "transfer",
								Version:           generateVersion(),
							})
							// Убрано добавление в CHAccountsRows - таблицы accounts больше нет
							// Убрано добавление в CHAccountsRows - таблицы accounts больше нет
						}
					}

					// Если нет записей в CHMoneyFlowRows, создаем простую запись на основе SendMax и delivered_amount
					if len(CHMoneyFlowRows) == 0 {
						from_currency := ""
						from_issuer := ""
						to_currency := ""
						to_issuer := ""
						from_amount := decimal.Zero
						to_amount := decimal.Zero

						// Определяем from_currency и from_issuer из SendMax
						if sm, ok := base["SendMax"].(map[string]interface{}); ok {
							if cur, ok := sm["currency"].(string); ok {
								from_currency = currencyToSymbol(cur)
							}
							if iss, ok := sm["issuer"].(string); ok {
								from_issuer = fixIssuerForXRP(from_currency, iss)
							}
						}

						// Определяем to_currency и to_issuer из delivered_amount или Amount
						if meta, ok := base["meta"].(map[string]interface{}); ok {
							if da, ok := meta["delivered_amount"].(map[string]interface{}); ok {
								if cur, ok := da["currency"].(string); ok {
									to_currency = currencyToSymbol(cur)
								}
								if iss, ok := da["issuer"].(string); ok {
									to_issuer = fixIssuerForXRP(to_currency, iss)
								}
								if vs, ok := da["value"].(string); ok {
									if _, ok := da["native"].(bool); ok {
										if v, err := decimal.NewFromString(vs); err == nil {
											to_amount = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
										}
									} else {
										to_amount, _ = decimal.NewFromString(vs)
									}
								}
							}
						}

						// Если delivered_amount не найден, используем Amount
						if to_currency == "" {
							if amountField, ok := base["Amount"].(map[string]interface{}); ok {
								if cur, ok := amountField["currency"].(string); ok {
									to_currency = currencyToSymbol(cur)
								}
								if iss, ok := amountField["issuer"].(string); ok {
									to_issuer = fixIssuerForXRP(to_currency, iss)
								}
								if vs, ok := amountField["value"].(string); ok {
									if _, ok := amountField["native"].(bool); ok {
										if v, err := decimal.NewFromString(vs); err == nil {
											to_amount = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
										}
									} else {
										to_amount, _ = decimal.NewFromString(vs)
									}
								}
							} else if amountStr, ok := base["Amount"].(string); ok {
								// Amount - это строка (XRP в drops)
								to_currency = "XRP"
								to_issuer = "XRP"
								if v, err := decimal.NewFromString(amountStr); err == nil {
									to_amount = v.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
								}
							}
						}

						// Если from_currency не найден, используем to_currency (для простых переводов)
						if from_currency == "" {
							from_currency = to_currency
							from_issuer = to_issuer
						}

						// Создаем запись только если есть валидные валюты
						if from_currency != "" && to_currency != "" {
							// Для простых переводов from_amount = to_amount
							from_amount = to_amount

							CHMoneyFlowRows = append(CHMoneyFlowRows, models.CHMoneyFlowRow{
								TxHash:            hash,
								LedgerIndex:       uint32(ledgerIndex),
								InLedgerIndex:     uint32(inLedgerIndex),
								CloseTimeUnix:     closeTimeUnix,
								FeeDrops:          feeDrops,
								FromAddress:       account,
								ToAddress:         destination,
								FromCurrency:      from_currency,
								FromIssuerAddress: from_issuer,
								ToCurrency:        to_currency,
								ToIssuerAddress:   to_issuer,
								FromAmount:        from_amount.Neg().String(),
								ToAmount:          to_amount.String(),
								InitFromAmount:    from_amount.Abs().String(),
								InitToAmount:      to_amount.Abs().String(),
								Quote:             "1",
								Kind:              "transfer",
								Version:           generateVersion(),
							})
						}
					}

					for _, row := range CHMoneyFlowRows {
						if row, err := json.Marshal(row); err == nil {
							_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHMoneyFlows(), Key: []byte(hash), Value: row})
						}
					}

					// Убрано отправка CHAccountsRows - таблицы accounts больше нет
				}
			}
		}
	})
}
