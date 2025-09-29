# XRPL Indexer - Тестирование с JSON файлами

Этот проект содержит модифицированную версию consumer'а для тестирования обработки XRPL транзакций с использованием локальных JSON файлов вместо Kafka.

## Описание

Файл `test_consumer.go` переделан для работы с JSON файлами из папки `examples/` для тестирования. Он имитирует работу оригинального consumer'а, но без использования Kafka, что позволяет тестировать логику обработки транзакций локально.

## Основные изменения

1. **Удалена зависимость от Kafka** - вместо чтения сообщений из Kafka, программа читает JSON файлы из папки
2. **Добавлена функция чтения файлов** - `ReadJSONFilesFromExamples()` автоматически находит и читает все JSON файлы
3. **Модифицирована логика обработки** - `ProcessTransaction()` обрабатывает отдельные транзакции
4. **Добавлен вывод результатов** - результаты сохраняются в JSON файл и выводятся в консоль
5. **Упрощена обработка денежных потоков** - убрана сложная логика с offers для упрощения тестирования

## Структура файлов

```
examples/
├── test_consumer.go      # Основной файл с логикой тестирования
├── consumer_example.go   # Оригинальный consumer (для справки)
├── tx_1.json            # Пример JSON файла с транзакцией
├── README.md            # Этот файл
└── test_results.json    # Результаты тестирования (создается автоматически)
```

## Использование

### Требования

- Go 1.19+
- JSON файлы транзакций XRPL в папке `examples/`
- Зависимости проекта (установлены через `go mod`)

### Запуск

```bash
# Перейти в папку examples
cd examples

# Запустить тестирование
go run test_consumer.go

# Показать справку
go run test_consumer.go -help
```

### Формат входных данных

Программа ожидает JSON файлы с транзакциями XRPL в формате:

```json
{
  "Account": "r4mA8izPxbhtD8hhb4sWoF8iFHrmKSuG9V",
  "TransactionType": "Payment",
  "Amount": {
    "currency": "5245414C00000000000000000000000000000000",
    "issuer": "rKVyXn1AhqMTvNA9hS6XkFjQNn2VE8Nz88",
    "value": "47124.56911526"
  },
  "Destination": "r4mA8izPxbhtD8hhb4sWoF8iFHrmKSuG9V",
  "Fee": "12",
  "hash": "17F9975016B36993CE217891F7B84247264A3EF16806980F19EE725A2267B03B",
  "meta": {
    "AffectedNodes": [...],
    "TransactionResult": "tesSUCCESS",
    "delivered_amount": {...}
  },
  ...
}
```

### Выходные данные

Программа создает файл `test_results.json` со следующей структурой:

```json
{
  "transactions": [
    {
      "tx_id": "uuid",
      "hash": "transaction_hash",
      "ledger_index": 98954359,
      "close_time_unix": 1640995200,
      "tx_type": "Payment",
      "account_id": "uuid",
      "destination_id": "uuid",
      "result": "tesSUCCESS",
      "fee_drops": 12,
      "raw_json": "..."
    }
  ],
  "accounts": [
    {
      "account_id": "uuid",
      "address": "r4mA8izPxbhtD8hhb4sWoF8iFHrmKSuG9V"
    }
  ],
  "assets": [
    {
      "asset_id": "uuid",
      "asset_type": "XRP",
      "currency": "XRP",
      "issuer_id": "00000000-0000-0000-0000-000000000000",
      "symbol": "XRP"
    }
  ],
  "money_flows": [
    {
      "tx_id": "uuid",
      "from_id": "uuid",
      "to_id": "uuid",
      "from_asset_id": "uuid",
      "to_asset_id": "uuid",
      "from_amount": "-47124.56911526",
      "to_amount": "47124.56911526",
      "quote": "1",
      "kind": "transfer"
    }
  ]
}
```

## Обрабатываемые типы транзакций

В настоящее время программа обрабатывает только транзакции типа `Payment`. Другие типы транзакций пропускаются.

## Особенности реализации

1. **Дедупликация активов и аккаунтов** - используется `sync.Map` для отслеживания уже обработанных элементов
2. **Обработка HEX валют** - автоматическое декодирование HEX-закодированных валютных кодов
3. **Генерация UUID** - детерминированная генерация UUID для транзакций, аккаунтов и активов
4. **Упрощенная обработка денежных потоков** - базовая логика без сложных алгоритмов сопоставления

## Отличия от оригинального consumer'а

1. **Нет Kafka** - работа с локальными файлами
2. **Упрощенная логика offers** - убрана сложная логика обработки ордеров
3. **Прямой вывод** - результаты сохраняются в файл вместо отправки в Kafka топики
4. **Тестовый режим** - фокус на тестировании логики, а не на производительности

## Возможные улучшения

1. Добавить поддержку других типов транзакций
2. Улучшить логику обработки денежных потоков
3. Добавить валидацию входных данных
4. Добавить метрики и статистику обработки
5. Добавить параллельную обработку файлов

## Примеры использования

```bash
# Тестирование с одним файлом
echo '{"TransactionType":"Payment",...}' > examples/test_tx.json
go run test_consumer.go

# Просмотр результатов
cat examples/test_results.json | jq '.transactions | length'
cat examples/test_results.json | jq '.accounts | length'
cat examples/test_results.json | jq '.assets | length'
cat examples/test_results.json | jq '.money_flows | length'
```
