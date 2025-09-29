package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/xrpscan/platform/indexer"
	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/platform/models"
)

// Глобальная переменная для отслеживания уже обработанных активов
var emittedAssets sync.Map

// Вспомогательные функции для генерации ID
func idTx(hash string) string { return uuid.NewSHA1(uuid.NameSpaceURL, []byte("tx:"+hash)).String() }
func idAccount(addr string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("acct:"+addr)).String()
}
func idAssetXRP() string { return uuid.NewSHA1(uuid.NameSpaceURL, []byte("asset:XRP::")).String() }
func idAssetIOU(currency, issuer string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("asset:IOU:"+currency+":"+issuer)).String()
}

// Декодирование HEX валюты
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

// Функция для красивого вывода JSON данных
func printPrettyJSON(data interface{}, label string) {
	fmt.Printf("--- %s ---\n", label)
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		fmt.Printf("Ошибка при форматировании JSON: %v\n", err)
		return
	}
	fmt.Println(string(jsonData))
	fmt.Println("---")
}

// Функция для вывода основных полей транзакции в читаемом формате
func printTransactionSummary(tx map[string]interface{}) {
	fmt.Println("=== СВОДКА ТРАНЗАКЦИИ ===")

	if hash, ok := tx["hash"].(string); ok {
		fmt.Printf("Hash: %s\n", hash)
	}
	if txType, ok := tx["TransactionType"].(string); ok {
		fmt.Printf("Тип: %s\n", txType)
	}
	if account, ok := tx["Account"].(string); ok {
		fmt.Printf("Отправитель: %s\n", account)
	}
	if destination, ok := tx["Destination"].(string); ok {
		fmt.Printf("Получатель: %s\n", destination)
	}
	if fee, ok := tx["Fee"].(string); ok {
		fmt.Printf("Комиссия: %s drops\n", fee)
	} else if fee, ok := tx["Fee"].(float64); ok {
		fmt.Printf("Комиссия: %.0f drops\n", fee)
	}
	if ledgerIndex, ok := tx["ledger_index"].(float64); ok {
		fmt.Printf("Индекс леджера: %.0f\n", ledgerIndex)
	}
	if date, ok := tx["date"].(float64); ok {
		fmt.Printf("Дата: %.0f\n", date)
	}

	// Выводим Amount
	if amount, ok := tx["Amount"].(map[string]interface{}); ok {
		fmt.Println("Сумма:")
		if value, ok := amount["value"].(string); ok {
			fmt.Printf("  Значение: %s\n", value)
		}
		if currency, ok := amount["currency"].(string); ok {
			fmt.Printf("  Валюта: %s\n", currency)
		}
		if issuer, ok := amount["issuer"].(string); ok {
			fmt.Printf("  Эмитент: %s\n", issuer)
		}
	} else if amount, ok := tx["Amount"].(string); ok {
		fmt.Printf("Сумма (XRP): %s drops\n", amount)
	}

	// Выводим SendMax
	if sendMax, ok := tx["SendMax"].(map[string]interface{}); ok {
		fmt.Println("Максимальная сумма отправки:")
		if value, ok := sendMax["value"].(string); ok {
			fmt.Printf("  Значение: %s\n", value)
		}
		if currency, ok := sendMax["currency"].(string); ok {
			fmt.Printf("  Валюта: %s\n", currency)
		}
		if issuer, ok := sendMax["issuer"].(string); ok {
			fmt.Printf("  Эмитент: %s\n", issuer)
		}
	} else if sendMax, ok := tx["SendMax"].(string); ok {
		fmt.Printf("Максимальная сумма отправки (XRP): %s drops\n", sendMax)
	}

	fmt.Println("=========================")
}

// Структуры для хранения результатов
type TestResults struct {
	Transactions []models.CHTransactionRow `json:"transactions"`
	Accounts     []models.CHAccountRow     `json:"accounts"`
	Assets       []models.CHAssetRow       `json:"assets"`
	MoneyFlows   []models.CHMoneyFlowRow   `json:"money_flows"`
}

// Функция для чтения всех JSON файлов из папки examples
func ReadJSONFilesFromExamples() ([]map[string]interface{}, error) {
	var transactions []map[string]interface{}

	// Ищем JSON файлы в текущей директории
	files, err := filepath.Glob("tx_1.json")
	if err != nil {
		return nil, fmt.Errorf("ошибка при поиске JSON файлов: %v", err)
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("не найдено JSON файлов в папке examples")
	}

	for _, file := range files {
		fmt.Printf("Обрабатываем файл: %s\n", file)

		data, err := ioutil.ReadFile(file)
		if err != nil {
			fmt.Printf("Ошибка чтения файла %s: %v\n", file, err)
			continue
		}

		var tx map[string]interface{}
		if err := json.Unmarshal(data, &tx); err != nil {
			fmt.Printf("Ошибка парсинга JSON файла %s: %v\n", file, err)
			continue
		}

		transactions = append(transactions, tx)
	}

	return transactions, nil
}

// Функция для обработки одной транзакции
func ProcessTransaction(tx map[string]interface{}) (*TestResults, error) {
	results := &TestResults{
		Transactions: make([]models.CHTransactionRow, 0),
		Accounts:     make([]models.CHAccountRow, 0),
		Assets:       make([]models.CHAssetRow, 0),
		MoneyFlows:   make([]models.CHMoneyFlowRow, 0),
	}

	// Фильтр по типу транзакции
	if tt, ok := tx["TransactionType"].(string); ok {
		if tt != "Payment" {
			return results, nil // Пропускаем нежелательные типы транзакций
		}
	} else {
		return results, nil // Пропускаем неизвестные или отсутствующие типы
	}

	// Модификация транзакции
	modified, err := indexer.ModifyTransaction(tx)
	if err != nil {
		return nil, fmt.Errorf("ошибка при модификации транзакции: %v", err)
	}

	b, err := json.Marshal(modified)
	if err != nil {
		return nil, fmt.Errorf("ошибка при маршалинге транзакции: %v", err)
	}

	// Извлечение базовых полей
	var base map[string]interface{} = modified
	hash, _ := base["hash"].(string)
	ledgerIndex, _ := base["ledger_index"].(float64)
	closeTime, _ := base["date"].(float64)
	account, _ := base["Account"].(string)
	destination, _ := base["Destination"].(string)
	result := ""
	if meta, ok := base["meta"].(map[string]interface{}); ok {
		if r, ok := meta["TransactionResult"].(string); ok {
			result = r
		}
	}

	feeDrops := uint64(0)

	switch v := base["Fee"].(type) {
	case float64:
		feeDrops = uint64(v)
	case int:
		feeDrops = uint64(v)
	case int64:
		feeDrops = uint64(v)
	case uint64:
		feeDrops = v
	case string:
		if parsed, err := strconv.ParseUint(v, 10, 64); err == nil {
			feeDrops = parsed
		}
	case json.Number:
		if parsed, err := v.Int64(); err == nil {
			feeDrops = uint64(parsed)
		}
	}

	// Создание записи транзакции
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
	results.Transactions = append(results.Transactions, txRow)

	// Добавление XRP актива (только один раз)
	if _, loaded := emittedAssets.LoadOrStore("XRP", true); !loaded {
		xrpRow := models.CHAssetRow{
			AssetID:   idAssetXRP(),
			AssetType: "XRP",
			Currency:  "XRP",
			IssuerID:  uuid.Nil.String(),
			Symbol:    "XRP",
		}
		results.Assets = append(results.Assets, xrpRow)
	}

	// Добавление аккаунтов
	if account != "" {
		aid := idAccount(account)
		if _, loaded := emittedAssets.LoadOrStore("acc:"+account, true); !loaded {
			ar := models.CHAccountRow{AccountID: aid, Address: account}
			results.Accounts = append(results.Accounts, ar)
		}
	}
	if destination != "" {
		did := idAccount(destination)
		if _, loaded := emittedAssets.LoadOrStore("acc:"+destination, true); !loaded {
			dr := models.CHAccountRow{AccountID: did, Address: destination}
			results.Accounts = append(results.Accounts, dr)
		}
	}

	// Обработка активов из полей транзакции
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
				results.Assets = append(results.Assets, assetRow)

				if _, loaded2 := emittedAssets.LoadOrStore("acc:"+iss, true); !loaded2 {
					ar := models.CHAccountRow{AccountID: issuerUUID, Address: iss}
					results.Accounts = append(results.Accounts, ar)
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
				results.Assets = append(results.Assets, assetRow)

				if _, loaded2 := emittedAssets.LoadOrStore("acc:"+iss, true); !loaded2 {
					ar := models.CHAccountRow{AccountID: issuerUUID, Address: iss}
					results.Accounts = append(results.Accounts, ar)
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
					results.Assets = append(results.Assets, assetRow)

					if _, loaded2 := emittedAssets.LoadOrStore("acc:"+iss, true); !loaded2 {
						ar := models.CHAccountRow{AccountID: issuerUUID, Address: iss}
						results.Accounts = append(results.Accounts, ar)
					}
				}
			}
		}
	}

	// Обработка денежных потоков из AffectedNodes
	if meta, ok := base["meta"].(map[string]interface{}); ok {
		if nodes, ok := meta["AffectedNodes"].([]interface{}); ok {
			// Первый проход: сбор изменений баланса (XRP и IOU) по аккаунту+активу

			printPrettyJSON(nodes, "AffectedNodes")

			if account == destination {
				for _, n := range nodes {
					node, _ := n.(map[string]interface{})
					if modified, ok := node["ModifiedNode"].(map[string]interface{}); ok {
						if final_fields, ok := modified["FinalFields"].(map[string]interface{}); ok {
							amount_1_prev := 0.
							amount_1_final := 0.

							amount_2_prev := 0.
							amount_2_final := 0.

							if node_account, ok := final_fields["Account"].(string); ok {
								if account == node_account {


									// Handle Balance field - can be string (XRP) or map (IOU)
									if balance, ok := final_fields["Balance"].(map[string]interface{}); ok {
										// IOU balance
										if vs, ok := balance["value"].(string); ok {
											amount_1_final, _ = strconv.ParseFloat(vs, 64)
										}
									} else if balanceStr, ok := final_fields["Balance"].(string); ok {
										// XRP balance
										if v, err := strconv.ParseFloat(balanceStr, 64); err == nil {
											amount_1_final = v / float64(models.DROPS_IN_XRP) // Convert drops to XRP
										}
									}

									fmt.Printf("amount_1_final: %f\n", amount_1_final)

									// Handle PreviousFields
									if previous_fields, ok := modified["PreviousFields"].(map[string]interface{}); ok {
										if balance, ok := previous_fields["Balance"].(map[string]interface{}); ok {
											// IOU balance
											if vs, ok := balance["value"].(string); ok {
												amount_1_prev, _ = strconv.ParseFloat(vs, 64)
											}
										} else if balanceStr, ok := previous_fields["Balance"].(string); ok {
											// XRP balance
											if v, err := strconv.ParseFloat(balanceStr, 64); err == nil {
												amount_1_prev = v / float64(models.DROPS_IN_XRP) // Convert drops to XRP
											}
										}
									}

									delta := amount_1_final - amount_1_prev + (float64(feeDrops) / float64(models.DROPS_IN_XRP))
									fmt.Printf("delta: %f\n", delta)
									fmt.Printf("(float64(feeDrops) / float64(models.DROPS_IN_XRP)): %f\n", (float64(feeDrops) / float64(models.DROPS_IN_XRP)))
									fmt.Printf("feeDrops: %d\n", feeDrops)
									fmt.Printf("amount_1_prev: %f\n", amount_1_prev)
									fmt.Printf("amount_1_final: %f\n", amount_1_final)


								}
							}

							if high_limit, ok := final_fields["HighLimit"].(map[string]interface{}); ok {
								if account == high_limit["issuer"].(string) {
									
								}
							}

						}
					}
				}

			}

			// Место для декс оферов (Илья)
			// Место для декс оферов (Илья)
			// Место для декс оферов (Илья)
			// Место для декс оферов (Илья)
			// Место для декс оферов (Илья)
			// Место для декс оферов (Илья)
			// Место для декс оферов (Илья)
			// Место для декс оферов (Илья)
			// Место для декс оферов (Илья)

			for _, n := range nodes {
				node, _ := n.(map[string]interface{})
				var modified map[string]interface{}
				var ok bool
				// Try ModifiedNode first, then DeletedNode
				if modified, ok = node["ModifiedNode"].(map[string]interface{}); !ok {
					modified, ok = node["DeletedNode"].(map[string]interface{})
				}
				if ok {
					if ledgerEntryType, ok := modified["LedgerEntryType"].(string); ok {
						if ledgerEntryType == "Offer" {
							if finalFields, ok := modified["FinalFields"].(map[string]interface{}); ok {
								if previousFields, ok := modified["PreviousFields"].(map[string]interface{}); ok {
									// Обрабатываем TakerGets из FinalFields
									if finalFieldsTakerGets, exists := finalFields["TakerGets"]; exists {
										fmt.Printf("=== FinalFields TakerGets ===\n")
										switch v := finalFieldsTakerGets.(type) {
										case string:
											fmt.Printf("Тип: XRP (строка), Значение: %s\n", v)
										case map[string]interface{}:
											fmt.Printf("Тип: Токен (объект)\n")
											if currency, ok := v["currency"].(string); ok {
												fmt.Printf("Currency: %s\n", currency)
											}
											if issuer, ok := v["issuer"].(string); ok {
												fmt.Printf("Issuer: %s\n", issuer)
											}
											if value, ok := v["value"].(string); ok {
												fmt.Printf("Value: %s\n", value)
											}
										default:
											fmt.Printf("Неизвестный тип: %T\n", v)
										}
										fmt.Println("========================")
									}

									// Обрабатываем TakerGets из PreviousFields
									if previousFieldsTakerGets, exists := previousFields["TakerGets"]; exists {
										fmt.Printf("=== PreviousFields TakerGets ===\n")
										switch v := previousFieldsTakerGets.(type) {
										case string:
											fmt.Printf("Тип: XRP (строка), Значение: %s\n", v)
										case map[string]interface{}:
											fmt.Printf("Тип: Токен (объект)\n")
											if currency, ok := v["currency"].(string); ok {
												fmt.Printf("Currency: %s\n", currency)
											}
											if issuer, ok := v["issuer"].(string); ok {
												fmt.Printf("Issuer: %s\n", issuer)
											}
											if value, ok := v["value"].(string); ok {
												fmt.Printf("Value: %s\n", value)
											}
										default:
											fmt.Printf("Неизвестный тип: %T\n", v)
										}
										fmt.Println("==========================")
									}

									// Обрабатываем TakerPays из FinalFields
									if finalFieldsTakerPays, exists := finalFields["TakerPays"]; exists {
										fmt.Printf("=== FinalFields TakerPays ===\n")
										switch v := finalFieldsTakerPays.(type) {
										case string:
											fmt.Printf("Тип: XRP (строка), Значение: %s\n", v)
										case map[string]interface{}:
											fmt.Printf("Тип: Токен (объект)\n")
											if currency, ok := v["currency"].(string); ok {
												fmt.Printf("Currency: %s\n", currency)
											}
											if issuer, ok := v["issuer"].(string); ok {
												fmt.Printf("Issuer: %s\n", issuer)
											}
											if value, ok := v["value"].(string); ok {
												fmt.Printf("Value: %s\n", value)
											}
										default:
											fmt.Printf("Неизвестный тип: %T\n", v)
										}
										fmt.Println("========================")
									}

									// Обрабатываем TakerPays из PreviousFields
									if previousFieldsTakerPays, exists := previousFields["TakerPays"]; exists {
										fmt.Printf("=== PreviousFields TakerPays ===\n")
										switch v := previousFieldsTakerPays.(type) {
										case string:
											fmt.Printf("Тип: XRP (строка), Значение: %s\n", v)
										case map[string]interface{}:
											fmt.Printf("Тип: Токен (объект)\n")
											if currency, ok := v["currency"].(string); ok {
												fmt.Printf("Currency: %s\n", currency)
											}
											if issuer, ok := v["issuer"].(string); ok {
												fmt.Printf("Issuer: %s\n", issuer)
											}
											if value, ok := v["value"].(string); ok {
												fmt.Printf("Value: %s\n", value)
											}
										default:
											fmt.Printf("Неизвестный тип: %T\n", v)
										}
										fmt.Println("==========================")
									}

									// Вычисляем дельту для владельца оффера
									if account, ok := finalFields["Account"].(string); ok {
										fmt.Printf("=== ДЕЛЬТА ДЛЯ ВЛАДЕЛЬЦА ОФФЕРА ===\n")
										fmt.Printf("Account: %s\n", account)

										// Получаем значения TakerGets (что получает владелец)
										var prevTakerGetsValue, finalTakerGetsValue decimal.Decimal
										var prevTakerGetsCurrency string
										var prevTakerGetsIssuer string

										// PreviousFields TakerGets
										if prevTakerGets, exists := previousFields["TakerGets"]; exists {
											switch v := prevTakerGets.(type) {
											case string:
												prevTakerGetsValue, _ = decimal.NewFromString(v)
												prevTakerGetsCurrency = "XRP"
											case map[string]interface{}:
												if currency, ok := v["currency"].(string); ok {
													prevTakerGetsCurrency = currency
												}
												if issuer, ok := v["issuer"].(string); ok {
													prevTakerGetsIssuer = issuer
												}
												if value, ok := v["value"].(string); ok {
													prevTakerGetsValue, _ = decimal.NewFromString(value)
												}
											}
										}

										// FinalFields TakerGets
										if finalTakerGets, exists := finalFields["TakerGets"]; exists {
											switch v := finalTakerGets.(type) {
											case string:
												finalTakerGetsValue, _ = decimal.NewFromString(v)
											case map[string]interface{}:
												if value, ok := v["value"].(string); ok {
													finalTakerGetsValue, _ = decimal.NewFromString(value)
												}
											}
										}

										// Получаем значения TakerPays (что отдает владелец)
										var prevTakerPaysValue, finalTakerPaysValue decimal.Decimal
										var prevTakerPaysCurrency string
										var prevTakerPaysIssuer string

										// PreviousFields TakerPays
										if prevTakerPays, exists := previousFields["TakerPays"]; exists {
											switch v := prevTakerPays.(type) {
											case string:
												prevTakerPaysValue, _ = decimal.NewFromString(v)
												prevTakerPaysCurrency = "XRP"
											case map[string]interface{}:
												if currency, ok := v["currency"].(string); ok {
													prevTakerPaysCurrency = currency
												}
												if issuer, ok := v["issuer"].(string); ok {
													prevTakerPaysIssuer = issuer
												}
												if value, ok := v["value"].(string); ok {
													prevTakerPaysValue, _ = decimal.NewFromString(value)
												}
											}
										}

										// FinalFields TakerPays
										if finalTakerPays, exists := finalFields["TakerPays"]; exists {
											switch v := finalTakerPays.(type) {
											case string:
												finalTakerPaysValue, _ = decimal.NewFromString(v)
											case map[string]interface{}:
												if value, ok := v["value"].(string); ok {
													finalTakerPaysValue, _ = decimal.NewFromString(value)
												}
											}
										}

										// Вычисляем дельты
										takerGetsDelta := prevTakerGetsValue.Sub(finalTakerGetsValue) // Что получил владелец
										takerPaysDelta := prevTakerPaysValue.Sub(finalTakerPaysValue) // Что отдал владелец

										fmt.Printf("TakerGets Delta (получил): %s %s\n", takerGetsDelta.String(), prevTakerGetsCurrency)
										if prevTakerGetsIssuer != "" {
											fmt.Printf("TakerGets Issuer: %s\n", prevTakerGetsIssuer)
										}

										fmt.Printf("TakerPays Delta (отдал): %s %s\n", takerPaysDelta.String(), prevTakerPaysCurrency)
										if prevTakerPaysIssuer != "" {
											fmt.Printf("TakerPays Issuer: %s\n", prevTakerPaysIssuer)
										}

										fmt.Println("=====================================")
									}
								}
							}
						}

					}
				}

			}

			type assetKey struct{ currency, issuer string }
			balances := make(map[string]map[assetKey]decimal.Decimal)

			for _, n := range nodes {
				node, _ := n.(map[string]interface{})
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
					high, _ := fields["HighLimit"].(map[string]interface{})
					low, _ := fields["LowLimit"].(map[string]interface{})
					bal, _ := fields["Balance"].(map[string]interface{})
					currency, _ := bal["currency"].(string)
					currency = normCurrency(currency)
					issuerHigh, _ := high["issuer"].(string)
					issuerLow, _ := low["issuer"].(string)

					// Определение изменений баланса
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
					abs := finalV.Sub(prevV)
					recv := issuerLow
					send := issuerHigh
					if abs.IsNegative() {
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
				}
			}

			// Создание рёбер для денежных потоков
			epsilon := decimal.New(1, -12) // 1e-12 to drop dust
			type edge struct {
				from, to string
				asset    assetKey
				amount   decimal.Decimal
			}
			edges := make([]edge, 0)

			// Сбор уникальных активов
			uniqueAssets := make(map[assetKey]struct{})
			for _, mm := range balances {
				for k := range mm {
					uniqueAssets[k] = struct{}{}
				}
			}

			for ak := range uniqueAssets {
				// Сбор источников и получателей для этого актива
				type pair struct {
					addr string
					amt  decimal.Decimal
				}
				sources := make([]pair, 0)
				sinks := make([]pair, 0)
				for addr, mm := range balances {
					amt := mm[ak]
					if amt.IsZero() {
						continue
					}
					if amt.IsNegative() {
						sources = append(sources, pair{addr, amt.Neg()})
					} else {
						sinks = append(sinks, pair{addr, amt})
					}
				}

				// Жадное сопоставление
				i, j := 0, 0
				for i < len(sources) && j < len(sinks) {
					s := sources[i]
					t := sinks[j]
					take := decimal.Min(s.amt, t.amt)
					if take.IsZero() || take.LessThan(epsilon) {
						break
					}
					edges = append(edges, edge{from: s.addr, to: t.addr, asset: ak, amount: take})
					s.amt = s.amt.Sub(take)
					t.amt = t.amt.Sub(take)
					if s.amt.IsZero() {
						i++
					} else {
						sources[i] = s
					}
					if t.amt.IsZero() {
						j++
					} else {
						sinks[j] = t
					}
				}
			}

			// Создание записей денежных потоков
			for _, e := range edges {
				kind := "transfer"
				accFrom := idAccount(e.from)
				accTo := idAccount(e.to)
				var fromAssetId string
				if e.asset.currency == "XRP" {
					fromAssetId = idAssetXRP()
				} else {
					fromAssetId = idAssetIOU(normCurrency(e.asset.currency), e.asset.issuer)
				}

				amtAbs := e.amount
				if amtAbs.IsNegative() {
					amtAbs = amtAbs.Neg()
				}
				if amtAbs.LessThan(epsilon) {
					continue
				}

				// sender (debit): from -> to
				mfSend := models.CHMoneyFlowRow{
					TxID:        txId,
					FromID:      accFrom,
					ToID:        accTo,
					FromAssetID: fromAssetId,
					ToAssetID:   fromAssetId, // Для простоты используем тот же актив
					FromAmount:  amtAbs.Neg().String(),
					ToAmount:    amtAbs.String(),
					Quote:       "1", // Для простоты
					Kind:        kind,
				}
				results.MoneyFlows = append(results.MoneyFlows, mfSend)
			}
		}
	}
	return results, nil
}

// Функция для записи результатов в файл
func WriteResultsToFile(results *TestResults, filename string) error {
	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return fmt.Errorf("ошибка при маршалинге результатов: %v", err)
	}

	if err := ioutil.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("ошибка при записи файла: %v", err)
	}

	return nil
}

// Функция для вывода результатов в консоль
func PrintResults(results *TestResults) {
	fmt.Printf("\n=== РЕЗУЛЬТАТЫ ОБРАБОТКИ ===\n")
	fmt.Printf("Транзакции: %d\n", len(results.Transactions))
	fmt.Printf("Аккаунты: %d\n", len(results.Accounts))
	fmt.Printf("Активы: %d\n", len(results.Assets))
	fmt.Printf("Денежные потоки: %d\n", len(results.MoneyFlows))

	if len(results.Transactions) > 0 {
		fmt.Printf("\n--- Пример транзакции ---\n")
		tx := results.Transactions[0]
		fmt.Printf("Hash: %s\n", tx.Hash)
		fmt.Printf("Type: %s\n", tx.TxType)
		fmt.Printf("Account: %s\n", tx.AccountID)
		fmt.Printf("Destination: %s\n", tx.DestinationID)
		fmt.Printf("Fee: %d drops\n", tx.FeeDrops)
	}

	if len(results.Accounts) > 0 {
		fmt.Printf("\n--- Пример аккаунта ---\n")
		acc := results.Accounts[0]
		fmt.Printf("ID: %s\n", acc.AccountID)
		fmt.Printf("Address: %s\n", acc.Address)
	}

	if len(results.Assets) > 0 {
		fmt.Printf("\n--- Пример актива ---\n")
		asset := results.Assets[0]
		fmt.Printf("ID: %s\n", asset.AssetID)
		fmt.Printf("Type: %s\n", asset.AssetType)
		fmt.Printf("Currency: %s\n", asset.Currency)
		fmt.Printf("Symbol: %s\n", asset.Symbol)
	}

	if len(results.MoneyFlows) > 0 {
		fmt.Printf("\n--- Пример денежного потока ---\n")
		mf := results.MoneyFlows[0]
		fmt.Printf("From: %s\n", mf.FromID)
		fmt.Printf("To: %s\n", mf.ToID)
		fmt.Printf("Amount: %s\n", mf.FromAmount)
		fmt.Printf("Kind: %s\n", mf.Kind)
	}
}

// Основная функция для запуска тестирования
func RunJSONFileTesting() error {
	fmt.Println("Запуск тестирования с JSON файлами...")

	// Инициализация логгера
	logger.New()

	// Чтение JSON файлов
	transactions, err := ReadJSONFilesFromExamples()
	if err != nil {
		return fmt.Errorf("ошибка при чтении JSON файлов: %v", err)
	}

	fmt.Printf("Найдено %d транзакций для обработки\n", len(transactions))

	// Агрегированные результаты
	var allResults TestResults
	allResults.Transactions = make([]models.CHTransactionRow, 0)
	allResults.Accounts = make([]models.CHAccountRow, 0)
	allResults.Assets = make([]models.CHAssetRow, 0)
	allResults.MoneyFlows = make([]models.CHMoneyFlowRow, 0)

	// Обработка каждой транзакции
	for i, tx := range transactions {
		fmt.Printf("Обрабатываем транзакцию %d/%d\n", i+1, len(transactions))

		// Выводим сводку транзакции в читаемом формате
		printTransactionSummary(tx)

		// Выводим полную транзакцию в JSON формате (раскомментируйте если нужно)
		// printPrettyJSON(tx, "ПОЛНАЯ ТРАНЗАКЦИЯ")

		results, err := ProcessTransaction(tx)
		if err != nil {
			fmt.Printf("Ошибка при обработке транзакции %d: %v\n", i+1, err)
			continue
		}

		// Объединение результатов
		allResults.Transactions = append(allResults.Transactions, results.Transactions...)
		allResults.Accounts = append(allResults.Accounts, results.Accounts...)
		allResults.Assets = append(allResults.Assets, results.Assets...)
		allResults.MoneyFlows = append(allResults.MoneyFlows, results.MoneyFlows...)
	}

	// Вывод результатов
	PrintResults(&allResults)

	// Запись в файл
	outputFile := "test_results.json"
	if err := WriteResultsToFile(&allResults, outputFile); err != nil {
		return fmt.Errorf("ошибка при записи результатов: %v", err)
	}

	fmt.Printf("\nРезультаты сохранены в файл: %s\n", outputFile)

	return nil
}

// Функция main для запуска тестирования
func main() {
	var help bool
	flag.BoolVar(&help, "help", false, "Показать справку")
	flag.BoolVar(&help, "h", false, "Показать справку")
	flag.Parse()

	if help {
		printHelp()
		return
	}

	fmt.Println("XRPL Indexer - Тестирование с JSON файлами")
	fmt.Println("==========================================")

	if err := RunJSONFileTesting(); err != nil {
		log.Fatalf("Ошибка при выполнении тестирования: %v", err)
	}

	fmt.Println("\nТестирование завершено успешно!")
}

func printHelp() {
	fmt.Println("XRPL Indexer - Тестирование с JSON файлами")
	fmt.Println("")
	fmt.Println("Использование:")
	fmt.Println("  go run test_consumer.go [опции]")
	fmt.Println("")
	fmt.Println("Опции:")
	fmt.Println("  -h, -help    Показать эту справку")
	fmt.Println("")
	fmt.Println("Описание:")
	fmt.Println("  Эта программа читает JSON файлы транзакций из папки examples/")
	fmt.Println("  и обрабатывает их, имитируя работу consumer'а из оригинального")
	fmt.Println("  проекта, но без использования Kafka.")
	fmt.Println("")
	fmt.Println("  Результаты сохраняются в файл examples/test_results.json")
	fmt.Println("  и выводятся в консоль.")
	fmt.Println("")
	fmt.Println("Требования:")
	fmt.Println("  - JSON файлы транзакций должны находиться в папке examples/")
	fmt.Println("  - Файлы должны содержать валидные JSON объекты транзакций XRPL")
	fmt.Println("")
	fmt.Println("Пример:")
	fmt.Println("  # Поместите JSON файлы в examples/")
	fmt.Println("  # Запустите программу")
	fmt.Println("  go run test_consumer.go")
}
