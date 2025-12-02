const fs = require('fs');
const path = require('path');

// Паттерн для поиска кириллицы
const CYRILLIC_PATTERN = /[А-Яа-яЁё]/;

// Директории и файлы для исключения
const EXCLUDE_PATTERNS = [
  '.git',
  'bin', // Скомпилированные бинарники
  'vendor', // Go vendor директория
  'coverage',
  '.cache',
  'find-cyrillic-files.js', // Исключаем сам скрипт
];

// Расширения файлов для проверки
const INCLUDE_EXTENSIONS = [
  '.go',
  '.js',
  '.json',
  '.md',
  '.txt',
  '.sql',
  '.xml',
  '.yml',
  '.yaml',
];

function shouldExclude(filePath) {
  const normalizedPath = filePath.replace(/\\/g, '/');
  return EXCLUDE_PATTERNS.some(pattern => normalizedPath.includes(pattern));
}

function findCyrillicLines(content) {
  const lines = content.split('\n');
  const cyrillicLines = [];
  
  lines.forEach((line, index) => {
    if (CYRILLIC_PATTERN.test(line)) {
      cyrillicLines.push(index + 1); // Нумерация строк с 1
    }
  });
  
  return cyrillicLines;
}

function findCyrillicFiles(dir, fileList = []) {
  const files = fs.readdirSync(dir);

  files.forEach(file => {
    const filePath = path.join(dir, file);
    const stat = fs.statSync(filePath);

    if (stat.isDirectory()) {
      if (!shouldExclude(filePath)) {
        findCyrillicFiles(filePath, fileList);
      }
    } else {
      if (shouldExclude(filePath)) {
        return;
      }

      const ext = path.extname(filePath).toLowerCase();
      const fileName = path.basename(filePath);
      
      // Пропускаем сам скрипт
      if (fileName === 'find-cyrillic-files.js') {
        return;
      }
      
      if (INCLUDE_EXTENSIONS.includes(ext) || ext === '') {
        try {
          const content = fs.readFileSync(filePath, 'utf8');
          const cyrillicLines = findCyrillicLines(content);
          
          if (cyrillicLines.length > 0) {
            // Относительный путь от корня проекта
            const relativePath = path.relative(process.cwd(), filePath);
            fileList.push({
              path: relativePath,
              lines: cyrillicLines
            });
          }
        } catch (error) {
          // Игнорируем ошибки чтения (бинарные файлы и т.д.)
          if (error.code !== 'EISDIR') {
            // Тихий режим - не выводим предупреждения
          }
        }
      }
    }
  });

  return fileList;
}

// Главная функция
function main() {
  const projectRoot = process.cwd();
  console.log(`Searching for files with Cyrillic characters in: ${projectRoot}\n`);
  console.log('Excluding:', EXCLUDE_PATTERNS.join(', '));
  console.log('Checking extensions:', INCLUDE_EXTENSIONS.join(', '));
  console.log('\n' + '='.repeat(60) + '\n');

  const startTime = Date.now();
  const filesWithCyrillic = findCyrillicFiles(projectRoot);
  const endTime = Date.now();

  if (filesWithCyrillic.length === 0) {
    console.log('✅ No files with Cyrillic characters found!');
  } else {
    console.log(`Found ${filesWithCyrillic.length} file(s) with Cyrillic characters:\n`);
    filesWithCyrillic.forEach((fileInfo, index) => {
      const linesStr = fileInfo.lines.length <= 10 
        ? fileInfo.lines.join(', ')
        : `${fileInfo.lines.slice(0, 10).join(', ')} ... (${fileInfo.lines.length} total)`;
      console.log(`${index + 1}. ${fileInfo.path}`);
      console.log(`   Lines: ${linesStr}\n`);
    });
  }

  console.log(`\n${'='.repeat(60)}`);
  console.log(`Search completed in ${endTime - startTime}ms`);
}

main();

