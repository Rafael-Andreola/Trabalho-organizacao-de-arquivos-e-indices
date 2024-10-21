using CsvHelper;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection.PortableExecutable;
using System.Text;
using System.Threading.Tasks;

namespace Trabalho1_OrganizaçõesDeArquivosE_Indices.Class
{
    public class FileHandler
    {
        private string _basePath;
        public FileHandler(string basePath)
        {
            _basePath = basePath;
        }

        public List<string> ProcessAndSaveSortedBlocks(string csvFilePath, string orderCriterium)
        {
            int bufferSize = 2500000;
            var tempFiles = new List<string>();
            var buffer = new List<Row>();
            int fileCount = 0;

            var stopwatch = new Stopwatch();

            using (var reader = new StreamReader($"{_basePath}\\{csvFilePath}"))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                csv.Read();  // Pula o cabeçalho
                csv.ReadHeader();

                TimeSpan timeElapsedTotal = new TimeSpan();

                stopwatch.Start();

                while (csv.Read())
                {
                    var row = new Row
                    {
                        productId = csv.GetField("product_id") ?? string.Empty,
                        categoryId = csv.GetField("category_id") ?? string.Empty,
                        brand = csv.GetField("brand") ?? string.Empty,
                        userId = csv.GetField("user_id") ?? string.Empty,
                        userSession = csv.GetField("user_session") ?? string.Empty,
                        eventType = csv.GetField("event_type") ?? string.Empty,
                    };

                    buffer.Add(row);

                    if (buffer.Count >= bufferSize)
                    {
                        // Grava o bloco ordenado em um arquivo temporário
                        string tmpFile = $"{_basePath}\\temp_{fileCount}.bin";

                        tempFiles.Add(tmpFile);

                        // Ordena o buffer
                        if(orderCriterium == "product")
                        {
                            WriteProductBufferToBinaryFile([.. buffer.OrderBy(p => long.Parse(p.productId))], tmpFile);
                        }
                        else if (orderCriterium == "user")
                        {
                            WriteUserBufferToBinaryFile([.. buffer.OrderBy(u => long.Parse(u.userId))], tmpFile);
                        }

                        Console.WriteLine($"File {fileCount} finalizado em {stopwatch.Elapsed.Subtract(timeElapsedTotal).Seconds}s");

                        timeElapsedTotal = stopwatch.Elapsed;

                        buffer.Clear();
                        fileCount++;
                    }
                }

                // Grava o último bloco restante
                if (buffer.Count > 0)
                {
                    string tmpFile = $"{_basePath}\\temp_{fileCount}.bin";
                    tempFiles.Add(tmpFile);

                    if(orderCriterium == "product")
                    {
                        WriteProductBufferToBinaryFile([.. buffer.OrderBy(p => long.Parse(p.productId))], tmpFile);
                    }
                        else if (orderCriterium == "user")
                    {
                        WriteUserBufferToBinaryFile([.. buffer.OrderBy(u => long.Parse(u.userId))], tmpFile);
                    }


                    Console.WriteLine($"File {fileCount} finalizado em {stopwatch.Elapsed.Subtract(timeElapsedTotal).Seconds}s");
                }

                stopwatch.Stop();

                Console.WriteLine($"Processo finalizado em {stopwatch.Elapsed}");
            }

            return tempFiles;
        }

        public List<string> ProcessAndSaveSortedBlocksFromBinary(string mainBinFilePath, string auxBinFilePath, string orderCriterium)
        {
            int bufferSize = 2500000; // Tamanho do buffer
            var tempFiles = new List<string>(); // Lista de arquivos temporários
            var buffer = new List<Row>(); // Buffer para armazenar registros
            int fileCount = 0; // Contador de arquivos temporários

            var stopwatch = new Stopwatch();
            stopwatch.Start();

            try
            {
                TimeSpan timeElapsedTotal = TimeSpan.Zero;

                // Processa o arquivo principal
                ProcessFile(mainBinFilePath, buffer, bufferSize, ref fileCount, tempFiles, orderCriterium, ref timeElapsedTotal, stopwatch, "main");

                // Se ainda restar dados no buffer após o arquivo principal
                if (buffer.Count > 0 || !string.IsNullOrEmpty(auxBinFilePath))
                {
                    // Processa o arquivo auxiliar se ele existir
                    ProcessFile(auxBinFilePath, buffer, bufferSize, ref fileCount, tempFiles, orderCriterium, ref timeElapsedTotal, stopwatch, "aux");
                }

                // Grava os dados restantes do buffer
                if (buffer.Count > 0)
                {
                    WriteBufferToTemporaryFile(buffer, fileCount, tempFiles, orderCriterium);
                    Console.WriteLine($"File {fileCount} finalizado em {stopwatch.Elapsed.Subtract(timeElapsedTotal).Seconds}s");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao processar: {ex.Message}");
            }
            finally
            {
                stopwatch.Stop();
                Console.WriteLine($"Processo finalizado em {stopwatch.Elapsed}");
            }

            return tempFiles;
        }

        // Método para processar um arquivo binário e armazenar em arquivos temporários
        private void ProcessFile(string filePath, List<Row> buffer, int bufferSize, ref int fileCount, List<string> tempFiles, string orderCriterium, ref TimeSpan timeElapsedTotal, Stopwatch stopwatch, string typeFile)
        {
            Row row = null;

            if (string.IsNullOrEmpty(filePath)) return;

            using (var fileStream = new FileStream($"{_basePath}\\{filePath}", FileMode.Open))
            using (var reader = new BinaryReader(fileStream))
            {
                while (fileStream.Position < fileStream.Length)
                {
                    // Pula o campo extra antes dos IDs
                    if (typeFile == "main")
                    {
                        reader.ReadBytes(15); // Ajuste conforme o tamanho do campo extra

                        row = ReadRow(reader, orderCriterium);

                        reader.ReadBytes(5);
                    }
                    else if (typeFile == "aux")
                    {
                        row = ReadRow(reader, orderCriterium);
                    }
                    
                    if (row.deleteField == "0") 
                    {
                        buffer.Add(row);
                    }

                    // Verifica se o buffer atingiu o tamanho limite
                    if (buffer.Count >= bufferSize)
                    {
                        WriteBufferToTemporaryFile(buffer, fileCount, tempFiles, orderCriterium);
                        Console.WriteLine($"File {fileCount} finalizado em {stopwatch.Elapsed.Subtract(timeElapsedTotal).Seconds}s");
                        timeElapsedTotal = stopwatch.Elapsed;

                        buffer.Clear();
                        fileCount++;
                    }
                }
            }
        }

        // Método para ler uma linha (Row) baseado no critério de ordenação
        private Row ReadRow(BinaryReader reader, string orderCriterium)
        {
            Row row = new Row();

            if (orderCriterium == "product")
            {
                row.productId = new string(reader.ReadChars(10)).Trim();
                row.categoryId = new string(reader.ReadChars(20)).Trim();
                row.brand = new string(reader.ReadChars(25)).Trim();
            }
            else if (orderCriterium == "user")
            {
                row.userId = new string(reader.ReadChars(10)).Trim();
                row.userSession = new string(reader.ReadChars(35)).Trim();
                row.eventType = new string(reader.ReadChars(10)).Trim();
            }

            row.deleteField = new string(reader.ReadChars(5)).Trim();

            return row;
        }

        // Método para gravar o buffer em um arquivo temporário ordenado
        private void WriteBufferToTemporaryFile(List<Row> buffer, int fileCount, List<string> tempFiles, string orderCriterium)
        {
            string tmpFile = $"{_basePath}\\temp_{fileCount}.bin";
            tempFiles.Add(tmpFile);

            if (orderCriterium == "product")
            {
                WriteProductBufferToBinaryFile([..buffer.OrderBy(p => long.Parse(p.productId))], tmpFile);
            }
            else if (orderCriterium == "user")
            {
                WriteUserBufferToBinaryFile([..buffer.OrderBy(u => long.Parse(u.userId))], tmpFile);
            }
        }


        // Função auxiliar para ler uma linha do arquivo binário
        private Row ReadRowFromBinary(BinaryReader reader)
        {
            return new Row
            {
                productId = new string(reader.ReadChars(15)).Trim(),
                categoryId = new string(reader.ReadChars(10)).Trim(),
                brand = new string(reader.ReadChars(35)).Trim(),
                userId = new string(reader.ReadChars(10)).Trim(),
                userSession = new string(reader.ReadChars(35)).Trim(),
                eventType = new string(reader.ReadChars(10)).Trim(),
            };
        }

        // Função auxiliar para ordenar e gravar o buffer em um arquivo binário
        private void WriteBufferToBinaryFile(List<Row> buffer, string tmpFile, string orderCriterium)
        {
            using (var writer = new BinaryWriter(File.Open(tmpFile, FileMode.Create)))
            {
                if (orderCriterium == "product")
                {
                    foreach (var row in buffer.OrderBy(p => long.Parse(p.productId)))
                    {
                        WriteRowToBinary(writer, row);
                    }
                }
                else if (orderCriterium == "user")
                {
                    foreach (var row in buffer.OrderBy(u => long.Parse(u.userId)))
                    {
                        WriteRowToBinary(writer, row);
                    }
                }
            }
        }

        // Função auxiliar para gravar uma linha no arquivo binário
        private void WriteRowToBinary(BinaryWriter writer, Row row)
        {
            writer.Write(row.productId.PadRight(15).ToCharArray());
            writer.Write(row.categoryId.PadRight(10).ToCharArray());
            writer.Write(row.brand.PadRight(35).ToCharArray());
            writer.Write(row.userId.PadRight(10).ToCharArray());
            writer.Write(row.userSession.PadRight(35).ToCharArray());
            writer.Write(row.eventType.PadRight(10).ToCharArray());
        }


        private void WriteProductBufferToBinaryFile(List<Row> productBuffer, string filePath)
        {
            using (var fileStream = new FileStream(filePath, FileMode.Create))
            using (var writer = new BinaryWriter(fileStream))
            {
                foreach (var item in productBuffer)
                {
                    writer.Write(item.productId.PadRight(10).AsSpan(0, 10));
                    writer.Write(item.categoryId.PadRight(20).AsSpan(0, 20));
                    writer.Write(item.brand.PadRight(25).AsSpan(0, 25));
                }
            }
        }

        private void WriteUserBufferToBinaryFile(List<Row> userBuffer, string filePath)
        {
            using (var fileStream = new FileStream(filePath, FileMode.Create))
            using (var writer = new BinaryWriter(fileStream))
            {
                foreach (var item in userBuffer)
                {
                    writer.Write(item.userId.PadRight(15).AsSpan(0, 15));
                    writer.Write(item.userSession.PadRight(35).AsSpan(0, 35));
                    writer.Write(item.eventType.PadRight(10).AsSpan(0, 10));
                }
            }
        }

        public void CreateProductData(List<string> tempFiles)
        {
            var readers = new List<BinaryReader>();

            try
            {
                // Abrir todos os arquivos temporários para leitura
                foreach (var tempFile in tempFiles)
                {
                    var fileStream = new FileStream(tempFile, FileMode.Open);
                    readers.Add(new BinaryReader(fileStream));
                }

                using var outputFileProduct = new FileStream($"{_basePath}\\Product.bin", FileMode.Create);

                using var writerProduct = new BinaryWriter(outputFileProduct);

                var priorityQueueProduct = new List<MergeProduct>();

                var stopwatch = new Stopwatch();
                stopwatch.Start();

                // Inicializa a fila com o primeiro registro de cada arquivo
                for (int i = 0; i < readers.Count; i++)
                {
                    if(TryReadRowForProductData(readers[i], out var row))
                    {
                        priorityQueueProduct.Add( new MergeProduct
                        {
                            row = row,
                            productIdReference = long.Parse(row.productId),
                            indexReader = i
                        });
                    }
                }

                Console.WriteLine("Iniciando Merge.");

                int j = 0;

                // Executa o merge
                while (priorityQueueProduct.Count > 0)
                {
                    // Remove o menor elemento da fila
                    MergeProduct firstProduct = null;
                    //MergeUser firstUser = null;

                    foreach (var item in priorityQueueProduct)
                    {
                        if(firstProduct == null || firstProduct.productIdReference >= item.productIdReference)
                        {
                            firstProduct = item;
                        }
                    }

                    var rowDataProduct = firstProduct.row;
                    var readerIndex = firstProduct.indexReader;

                    // Grava o produto no arquivo final
                    writerProduct.Write((j++).ToString().PadRight(15).AsSpan(0, 15));
                    writerProduct.Write(rowDataProduct.productId.PadRight(10).AsSpan(0, 10));
                    writerProduct.Write(rowDataProduct.categoryId.PadRight(20).AsSpan(0, 20));
                    writerProduct.Write(rowDataProduct.brand.PadRight(25).AsSpan(0, 25));
                    writerProduct.Write(0.ToString().PadRight(5).AsSpan(0, 5));
                    writerProduct.Write("\n".ToString().PadLeft(5).AsSpan(0, 5));

                    priorityQueueProduct.Remove(firstProduct);

                    // Lê o próximo produto do arquivo correspondente
                    if (TryReadRowForProductData(readers[readerIndex], out var row))
                    {
                        if(row == null || row.productId == "")
                        {
                            continue;
                        }

                        priorityQueueProduct.Add(new MergeProduct
                        {

                            row = row,
                            productIdReference = long.Parse(row.productId),
                            indexReader = readerIndex
                        });

                    }
                }

                stopwatch.Stop();

                Console.WriteLine(stopwatch.Elapsed);
            }
            finally
            {
                // Fechar todos os leitores
                foreach (var reader in readers)
                {
                    reader.Close();
                    
                }

                foreach (var tempFile in tempFiles)
                {
                    try
                    {
                        File.Delete(tempFile);
                        Console.WriteLine($"Arquivo {tempFile} excluído com sucesso.");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Erro ao excluir o arquivo {tempFile}: {ex.Message}");
                    }
                }
            }
        }

        public void CreateUserData(List<string> tempFiles)
        {
            var readers = new List<BinaryReader>();

            try
            {
                // Abrir todos os arquivos temporários para leitura
                foreach (var tempFile in tempFiles)
                {
                    var fileStream = new FileStream(tempFile, FileMode.Open);
                    readers.Add(new BinaryReader(fileStream));
                }

                using var outputFileUser = new FileStream($"{_basePath}\\User.bin", FileMode.Create);

                using var writerUser = new BinaryWriter(outputFileUser);

                var priorityQueueUser = new List<MergeUser>();

                var stopwatch = new Stopwatch();
                stopwatch.Start();

                // Inicializa a fila com o primeiro registro de cada arquivo
                for (int i = 0; i < readers.Count; i++)
                {
                    if (TryReadRowForUserData(readers[i], out var row))
                    {
                        priorityQueueUser.Add(new MergeUser
                        {
                            row = row,
                            userIdReference = long.Parse(row.userId),
                            indexReader = i
                        });
                    }
                }

                Console.WriteLine("Iniciando Merge.");

                int j = 0;

                // Executa o merge
                while (priorityQueueUser.Count > 0)
                {
                    // Remove o menor elemento da fila
                    MergeUser firstUser = null;

                    foreach (var item in priorityQueueUser)
                    {
                        if (firstUser == null || firstUser.userIdReference >= item.userIdReference)
                        {
                            firstUser = item;
                        }
                    }

                    var rowDataUser = firstUser.row;
                    var readerIndex = firstUser.indexReader;

                    writerUser.Write((j++).ToString().PadRight(15).AsSpan(0, 15));
                    writerUser.Write(rowDataUser.userId.PadRight(10).AsSpan(0, 10));
                    writerUser.Write(rowDataUser.userSession.PadRight(35).AsSpan(0, 35));
                    writerUser.Write(rowDataUser.eventType.PadRight(10).AsSpan(0, 10));
                    writerUser.Write(0.ToString().PadRight(5).AsSpan(0, 5));
                    writerUser.Write("\n".ToString().PadLeft(5).AsSpan(0, 5));

                    priorityQueueUser.Remove(firstUser);

                    if (TryReadRowForUserData(readers[readerIndex], out var row))
                    {
                        if (row == null || row.userId == "")
                        {
                            continue;
                        }

                        priorityQueueUser.Add(new MergeUser
                        {
                            row = row,
                            userIdReference = long.Parse(row.userId),
                            indexReader = readerIndex
                        });

                    }
                }

                stopwatch.Stop();

                Console.WriteLine(stopwatch.Elapsed);
            }
            finally
            {
                // Fechar todos os leitores
                foreach (var reader in readers)
                {
                    reader.Close();

                }

                foreach (var tempFile in tempFiles)
                {
                    try
                    {
                        File.Delete(tempFile);
                        Console.WriteLine($"Arquivo {tempFile} excluído com sucesso.");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Erro ao excluir o arquivo {tempFile}: {ex.Message}");
                    }
                }
            }
        }

        private bool TryReadRowForProductData(BinaryReader reader, out Row row)
        {
            try
            {
                string productId = new string(reader.ReadChars(10)).Trim();
                string categoryId = new string(reader.ReadChars(20)).Trim();
                string brand = new string(reader.ReadChars(25)).Trim();
                
                // Cria um novo objeto ProductData com os dados lidos
                row = new Row
                {
                    //Id = long.Parse(id),
                    productId = productId,
                    categoryId = categoryId,
                    brand = brand,
                };

                return true; // Leitura bem-sucedida
            }
            catch (EndOfStreamException)
            {
                row = null; // Final do arquivo alcançado
                return false;
            }
            catch (Exception ex)
            {
                // Tratamento para outros erros potenciais
                Console.WriteLine($"Erro ao ler os dados: {ex.Message}");
                row = null;
                return false;
            }
        }

        private bool TryReadRowForUserData(BinaryReader reader, out Row row)
        {
            try
            {
                // Lê os dados do arquivo em tamanhos fixos
                string userId = new string(reader.ReadChars(15)).Trim();
                string userSession = new string(reader.ReadChars(35)).Trim();
                string eventType = new string(reader.ReadChars(10)).Trim();

                // Cria um novo objeto ProductData com os dados lidos
                row = new Row
                {
                    userId = userId,
                    userSession = userSession,
                    eventType = eventType
                };

                return true; // Leitura bem-sucedida
            }
            catch (EndOfStreamException)
            {
                row = null; // Final do arquivo alcançado
                return false;
            }
            catch (Exception ex)
            {
                // Tratamento para outros erros potenciais
                Console.WriteLine($"Erro ao ler os dados: {ex.Message}");
                row = null;
                return false;
            }
        }
        
        public void showDataBinaryFile(string binaryFilePath)
        {
            using FileStream file = new FileStream($"{_basePath}\\{binaryFilePath}.bin", FileMode.Open);

            using var reader = new BinaryReader(file);

            reader.BaseStream.Position = 0;

            while (reader.BaseStream.Position < reader.BaseStream.Length) 
            {
                string text = "";
                while (true)
                {
                    char c = reader.ReadChar();

                    if(c != '\n')
                    {
                        text += c.ToString();
                        continue;
                    }

                    Console.WriteLine(text);
                    reader.BaseStream.Seek(reader.BaseStream.Position + text.Length, SeekOrigin.Begin);
                    break;
                }
            }
        }

        public void CreateIndex(string binFilePath, string indexFileName)
        {
            using var fileStream = new FileStream($"{_basePath}\\{binFilePath}", FileMode.Open);
            using var reader = new BinaryReader(fileStream);

            using var indexFileStream = new FileStream($"{_basePath}\\{indexFileName}", FileMode.Create);
            using var indexWriter = new BinaryWriter(indexFileStream);

            string previousId = string.Empty;
            string autoIncremt = string.Empty;

            var stopwatch = new Stopwatch();

            stopwatch.Start();

            try
            {
                long i = 0;

                while (fileStream.Position < fileStream.Length)
                {
                    reader.BaseStream.Position = (80 * i++); // Pula os outros campos

                    string currentId;

                    autoIncremt = new string(reader.ReadChars(15)).Trim();
                    currentId = new string(reader.ReadChars(10)).Trim();

                    //reader.ReadString();
                    //reader.BaseStream.Seek(55, SeekOrigin.Current); // Pula os outros campos

                    if (currentId != previousId)
                    {
                        indexWriter.Write(currentId.PadRight(10).AsSpan(0, 10)); 
                        indexWriter.Write(autoIncremt.PadRight(15).AsSpan(0, 15));
                        indexWriter.Write("\n");

                        // Atualizar o id anterior para o atual
                        previousId = currentId;
                    }
                }

                stopwatch.Stop();

                Console.WriteLine($"Arquivo de índice criado com sucesso, finalizado em {stopwatch.Elapsed}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao criar o arquivo de índice: {ex.Message}");
            }
        }

        public void ProductWithMoreInteraction(string indexFileName)
        {
            string? previousProductId = null;
            long? previousPosition = null;

            string? maxProductId = null;
            long maxRecordCount = 0;

            using var indexFileStream = new FileStream($"{_basePath}\\{indexFileName}", FileMode.Open);
            using var indexReader = new StreamReader(indexFileStream);

            string? line;
            while ((line = indexReader.ReadLine()) != null)
            {
                string productId = line.Substring(0, 10).Trim();
                long currentPosition = long.Parse(line.Substring(10, 15).Trim());

                if (previousProductId != null)
                {
                    long recordCount = currentPosition - previousPosition.GetValueOrDefault();

                    if (recordCount > maxRecordCount)
                    {
                        maxRecordCount = recordCount;
                        maxProductId = previousProductId;
                    }
                }
                previousProductId = productId;
                previousPosition = currentPosition;
            }

            if (maxProductId != null)
            {
                Console.WriteLine($"O produto com mais registros é '{maxProductId}' com {maxRecordCount} registros.");
            }
            else
            {
                Console.WriteLine("Nenhum produto encontrado no arquivo de índice.");
            }
        }

        public void UserWithMoreInteraction(string indexFileName)
        {
            string? previousUserId = null;
            long? previousPosition = null;

            string? userWithMoreInteraction = null;
            long maxRecordCount = 0;

            using var indexFileStream = new FileStream($"{_basePath}\\{indexFileName}", FileMode.Open);
            using var indexReader = new StreamReader(indexFileStream);

            string? line;
            while ((line = indexReader.ReadLine()) != null)
            {
                string userId = line.Substring(0, 10).Trim();
                long currentPosition = long.Parse(line.Substring(10, 15).Trim());

                if (previousUserId != null)
                {
                    long recordCount = currentPosition - previousPosition.GetValueOrDefault();

                    if (recordCount > maxRecordCount)
                    {
                        maxRecordCount = recordCount;
                        userWithMoreInteraction = previousUserId;
                    }
                }
                previousUserId = userId;
                previousPosition = currentPosition;
            }

            if (userWithMoreInteraction != null)
            {
                Console.WriteLine($"O Usuário com mais interações é '{userWithMoreInteraction}' com {maxRecordCount} registros.");
            }
            else
            {
                Console.WriteLine("Nenhum usuário encontrado no arquivo de índice.");
            }
        }

        public void FindProductId(string productId, string fileName)
        {
            // Verifica se o arquivo existe
            if (File.Exists($"{_basePath}\\{fileName}"))
            {
                // Aplicar a pesquisa binária diretamente no arquivo
                long indiceEncontrado = PesquisaBinariaArquivo($"{_basePath}\\{fileName}", productId, 27);

                if (indiceEncontrado != -1)
                {
                    Console.WriteLine($"Product ID '{productId}' encontrado no registro {indiceEncontrado}.");
                }
                else
                {
                    Console.WriteLine($"Product ID '{productId}' não encontrado.");
                }
            }
            else
            {
                Console.WriteLine("Arquivo não encontrado.");
            }

        }
        public void FindUserId(string userId, string fileName)
        {
            // Verifica se o arquivo existe
            if (File.Exists($"{_basePath}\\{fileName}"))
            {
                // Aplicar a pesquisa binária diretamente no arquivo
                long indiceEncontrado = PesquisaBinariaArquivo($"{_basePath}\\{fileName}", userId, 27);

                if (indiceEncontrado != -1)
                {
                    Console.WriteLine($"User ID '{userId}' encontrado no registro {indiceEncontrado}.");
                }
                else
                {
                    Console.WriteLine($"User ID '{userId}' não encontrado.");
                }
            }
            else
            {
                Console.WriteLine("Arquivo não encontrado.");
            }

        }

        // Função para ler um registro em uma posição específica do arquivo
        public (string productId, string end) LerRegistro(FileStream fs, long posicao)
        {
            // Cada registro tem 25 bytes: 10 bytes para o productId, 15 bytes para o end
            byte[] buffer = new byte[25];

            // Mover o ponteiro do arquivo para a posição correta
            fs.Seek(posicao, SeekOrigin.Begin);

            // Ler 25 bytes (1 registro)
            fs.Read(buffer, 0, 25);

            // Extrair o productId (primeiros 10 bytes) e end (próximos 15 bytes)
            string register = Encoding.ASCII.GetString(buffer, 0, 10).Trim();
            string end = Encoding.ASCII.GetString(buffer, 10, 15).Trim();

            return (register, end);
        }

        // Função de pesquisa binária direto no arquivo
        public long PesquisaBinariaArquivo(string caminho, string id, long tamanhoRegistro)
        {
            // Abrir o arquivo em modo de leitura
            using (FileStream fs = new FileStream(caminho, FileMode.Open, FileAccess.Read))
            {
                long esquerda = 0;
                long direita = fs.Length / tamanhoRegistro - 1;

                while (esquerda <= direita)
                {
                    long meio = (esquerda + direita) / 2;
                    long posicao = meio * tamanhoRegistro;

                    // Ler o registro na posição 'meio'
                    var (register, _) = LerRegistro(fs, posicao);

                    // Comparar o productId
                    int comparacao = string.Compare(register, id, StringComparison.Ordinal);

                    if (comparacao == 0)
                    {
                        return meio;  // ProductId encontrado, retorna o índice do registro
                    }
                    else if (comparacao < 0)
                    {
                        esquerda = meio + 1;
                    }
                    else
                    {
                        direita = meio - 1;
                    }
                }

                return -1;  // ProductId não encontrado
            }
        }

        public bool DeleteByProductId(string file, string idx, int bufferLength)
        {
            // Verifica se o arquivo existe
            if (File.Exists($"{_basePath}\\{file}"))
            {
                using (FileStream fs = new FileStream($"{_basePath}\\{file}", FileMode.Open, FileAccess.ReadWrite))
                {
                    long position = (long.Parse(idx) * bufferLength);

                    if (fs.Position < position)
                    {
                        Console.WriteLine("Id não encontrado.");
                        return false;
                    }

                    byte[] buffer = new byte[bufferLength];

                    // Mover o ponteiro do arquivo para a posição correta
                    fs.Seek(position, SeekOrigin.Begin);

                    // Ler 25 bytes (1 registro)
                    fs.Read(buffer, 0, bufferLength);

                    // Extrair o productId (primeiros 10 bytes) e end (próximos 15 bytes)
                    string id = Encoding.ASCII.GetString(buffer, 0, 15).Trim();
                    string productId = Encoding.ASCII.GetString(buffer, 15, 10).Trim();
                    string categoryId = Encoding.ASCII.GetString(buffer, 25, 20).Trim();
                    string brand = Encoding.ASCII.GetString(buffer, 45, 25).Trim();

                    fs.Seek(position, SeekOrigin.Begin);
                    BinaryWriter bw = new BinaryWriter(fs);

                    bw.Write(id.PadRight(15).AsSpan(0, 15));
                    bw.Write(productId.PadRight(10).AsSpan(0, 10));
                    bw.Write(categoryId.PadRight(20).AsSpan(0, 20));
                    bw.Write(brand.PadRight(25).AsSpan(0, 25));
                    bw.Write(1.ToString().PadRight(5).AsSpan(0, 5));
                    bw.Write("\n".ToString().PadLeft(5).AsSpan(0, 5));

                }

                Console.WriteLine($"ID '{idx}' deletado {idx}.");
            }
            else
            {
                Console.WriteLine($"ID '{idx}' não encontrado.");
                return false;
            }

            return true;
        }

        public bool DeleteByUserId(string file, string idx, int bufferLength)
        {
            // Verifica se o arquivo existe
            if (File.Exists($"{_basePath}\\{file}"))
            {
                using (FileStream fs = new FileStream($"{_basePath}\\{file}", FileMode.Open, FileAccess.ReadWrite))
                {
                    long position = (long.Parse(idx) * bufferLength);

                    if (fs.Length < position)
                    {
                        Console.WriteLine("Id não encontrado.");
                        return false;
                    }

                    byte[] buffer = new byte[bufferLength];

                    // Mover o ponteiro do arquivo para a posição correta
                    fs.Seek(position, SeekOrigin.Begin);

                    // Ler 25 bytes (1 registro)
                    fs.Read(buffer, 0, bufferLength);

                    // Extrair o productId (primeiros 10 bytes) e end (próximos 15 bytes)
                    fs.Seek(position, SeekOrigin.Begin);
                    BinaryWriter bw = new BinaryWriter(fs);

                    bw.Write(Encoding.ASCII.GetString(buffer, 0, 15).Trim().PadRight(15).AsSpan(0, 15));
                    bw.Write(Encoding.ASCII.GetString(buffer, 15, 10).Trim().PadRight(10).AsSpan(0, 10));
                    bw.Write(Encoding.ASCII.GetString(buffer, 25, 35).Trim().PadRight(35).AsSpan(0, 35));
                    bw.Write(Encoding.ASCII.GetString(buffer, 60, 10).PadRight(10).AsSpan(0, 10));
                    bw.Write(1.ToString().PadRight(5).AsSpan(0, 5));
                    bw.Write("\n".ToString().PadLeft(5).AsSpan(0, 5));

                }

                Console.WriteLine($"ID '{idx}' deletado {idx}.");
            }
            else
            {
                Console.WriteLine($"ID '{idx}' não encontrado.");
                return false;
            }

            return true;
        }

        private bool TryReadRow(BinaryReader reader, string indexField, out string id, out string autoIncrement, out string exclusionFlag)
        {
            try
            {
                if (reader.BaseStream.Position >= reader.BaseStream.Length)
                {
                    id = autoIncrement = exclusionFlag = string.Empty;
                    return false;
                }

                autoIncrement = new string(reader.ReadChars(15)).Trim();  // Autoincremento
                id = new string(reader.ReadChars(10)).Trim();  // ID (userId ou productId)

                if (indexField == "user")
                {
                    reader.BaseStream.Seek(45, SeekOrigin.Current); // Pula os campos userSession, eventType e outros
                }
                else if (indexField == "product")
                {
                    reader.BaseStream.Seek(45, SeekOrigin.Current); // Pula os campos categoryId, brand e outros
                }

                exclusionFlag = new string(reader.ReadChars(5)).Trim();  // Campo de exclusão
                reader.BaseStream.Seek(5, SeekOrigin.Current);  // Pula o '\n'
                return true;
            }
            catch
            {
                id = autoIncrement = exclusionFlag = string.Empty;
                return false;
            }
        }

        private void WriteRow(BinaryWriter writer, string id, string autoIncrement, BinaryReader reader)
        {
            writer.Write(autoIncrement.PadRight(15).AsSpan(0, 15));
            writer.Write(id.PadRight(10).AsSpan(0, 10));

            char[] buffer = reader.ReadChars(45);
            writer.Write(buffer);

            string exclusionFlag = new string(reader.ReadChars(5)).Trim();

            reader.BaseStream.Seek(5, SeekOrigin.Current);
            writer.Write("\n".ToString().PadRight(5).AsSpan(0, 5));
        }

        public void InsertUserIntoAuxFile(string auxFilePath, UserData newUser)
        {
            using var auxFileStream = new FileStream($"{_basePath}\\{auxFilePath}", FileMode.Append);
            using var auxWriter = new BinaryWriter(auxFileStream);

            auxWriter.Write(newUser.userId.PadRight(15).AsSpan(0, 15));
            auxWriter.Write(newUser.userSession.PadRight(35).AsSpan(0, 35));
            auxWriter.Write(newUser.eventType.PadRight(10).AsSpan(0, 10));    
            auxWriter.Write("\n".ToString().PadLeft(5).AsSpan(0, 5));             

            Console.WriteLine("Registro de usuário inserido no arquivo auxiliar com sucesso.");
        }

        public void InsertProductIntoAuxFile(string auxFilePath, ProductData newProduct)
        {
            using var auxFileStream = new FileStream($"{_basePath}\\{auxFilePath}", FileMode.Append);
            using var auxWriter = new BinaryWriter(auxFileStream);

            auxWriter.Write(newProduct.productId.PadRight(10).AsSpan(0, 10));
            auxWriter.Write(newProduct.categoryId.PadRight(20).AsSpan(0, 20));
            auxWriter.Write(newProduct.brand.PadRight(25).AsSpan(0, 10));
            auxWriter.Write("\n".ToString().PadLeft(5).AsSpan(0, 5));

            Console.WriteLine("Registro de usuário inserido no arquivo auxiliar com sucesso.");
        }
    }
}
