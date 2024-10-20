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
            int bufferSize = 5000000;
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
            int bufferSize = 5000000; // Tamanho do buffer
            var tempFiles = new List<string>(); // Lista para armazenar arquivos temporários
            var buffer = new List<Row>(); // Buffer para armazenar registros lidos
            int fileCount = 0; // Contador de arquivos temporários

            var stopwatch = new Stopwatch();
            TimeSpan timeElapsedTotal = new TimeSpan();
            stopwatch.Start();

            try
            {
                using (var mainFileStream = new FileStream($"{_basePath}\\{mainBinFilePath}", FileMode.Open))
                using (var reader = new BinaryReader(mainFileStream))
                {
                    while (mainFileStream.Position < mainFileStream.Length)
                    {
                        var row = ReadRowFromBinary(reader);

                        buffer.Add(row);

                        if (buffer.Count >= bufferSize)
                        {
                            // Grava o buffer em um arquivo temporário
                            string tmpFile = $"{_basePath}\\temp_{fileCount}.bin";
                            tempFiles.Add(tmpFile);

                            // Ordena e grava o buffer em um arquivo binário temporário
                            WriteBufferToBinaryFile(buffer, tmpFile, orderCriterium);

                            Console.WriteLine($"File {fileCount} finalizado em {stopwatch.Elapsed.Subtract(timeElapsedTotal).Seconds}s");
                            timeElapsedTotal = stopwatch.Elapsed;

                            buffer.Clear();
                            fileCount++;
                        }
                    }
                }

                // Se ainda restar dados no buffer após o processamento do arquivo principal
                if (buffer.Count > 0)
                {
                    // Agora continua lendo do arquivo auxiliar
                    using (var auxFileStream = new FileStream($"{_basePath}\\{auxBinFilePath}", FileMode.Open))
                    using (var auxReader = new BinaryReader(auxFileStream))
                    {
                        while (auxFileStream.Position < auxFileStream.Length)
                        {
                            var row = ReadRowFromBinary(auxReader); // Lê os dados do arquivo auxiliar
                            buffer.Add(row);

                            if (buffer.Count >= bufferSize)
                            {
                                string tmpFile = $"{_basePath}\\temp_{fileCount}.bin";
                                tempFiles.Add(tmpFile);

                                // Ordena e grava o buffer em um arquivo binário temporário
                                WriteBufferToBinaryFile(buffer, tmpFile, orderCriterium);

                                Console.WriteLine($"File {fileCount} finalizado em {stopwatch.Elapsed.Subtract(timeElapsedTotal).Seconds}s");
                                timeElapsedTotal = stopwatch.Elapsed;

                                buffer.Clear();
                                fileCount++;
                            }
                        }
                    }

                    // Grava os últimos dados restantes
                    if (buffer.Count > 0)
                    {
                        string tmpFile = $"{_basePath}\\temp_{fileCount}.bin";
                        tempFiles.Add(tmpFile);

                        WriteBufferToBinaryFile(buffer, tmpFile, orderCriterium);
                        Console.WriteLine($"File {fileCount} finalizado em {stopwatch.Elapsed.Subtract(timeElapsedTotal).Seconds}s");
                    }
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

        public List<ProductData> ReadFromProductDataFile(string binaryFilePath)
        {
            var productDataList = new List<ProductData>();

            using (var fileStream = new FileStream($"{_basePath}\\{binaryFilePath}", FileMode.Open))
            using (var reader = new BinaryReader(fileStream))
            {
                while (fileStream.Position < fileStream.Length)
                {
                    var productId = reader.ReadString();
                    var categoryId = reader.ReadString();
                    var brand = reader.ReadString();

                    productDataList.Add(new ProductData
                    {
                        productId = productId,
                        categoryId = categoryId,
                        brand = brand
                    });
                }
            }

            return productDataList;
        }

        public void SaveSortedProductData(string binaryFilePath, List<ProductData> productDataList)
        {
            using (var fileStream = new FileStream($"{_basePath}\\{binaryFilePath}", FileMode.Create))
            using (var writer = new BinaryWriter(fileStream))
            {
                foreach (var product in productDataList)
                {
                    writer.Write(product.productId);
                    writer.Write(product.categoryId);
                    writer.Write(product.brand);
                }
            }
        }

        public void SortAndSaveProductDataBinaryFile(string binaryFilePath)
        {
            var productDataList = ReadFromProductDataFile(binaryFilePath);

            var sortedData = productDataList.OrderBy(p => p.productId).ToList();

            SaveSortedProductData(binaryFilePath, sortedData);
        }

        //TODO: Fazer certo
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

        public List<string> GetBinaryReaders(int num)
        {
            //TODO: fazer certo
            List<string> tempFiles = new List<string>();

            for (int i = 0; i < num; i++)
            {
                tempFiles.Add($"{_basePath}\\temp_{i}.bin");
            }

            return tempFiles;
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

            TimeSpan timeElapsedTotal = new TimeSpan();

            try
            {
                while (fileStream.Position < fileStream.Length)
                {
                    string currentId;

                    autoIncremt = new string(reader.ReadChars(15)).Trim();
                    currentId = new string(reader.ReadChars(10)).Trim();

                    //reader.ReadString();
                    reader.BaseStream.Seek(55, SeekOrigin.Current); // Pula os outros campos

                    if (currentId != previousId)
                    {
                        indexWriter.Write(currentId.PadRight(10).AsSpan(0, 10)); 
                        indexWriter.Write(autoIncremt.PadRight(15).AsSpan(0, 15));
                        indexWriter.Write("\n");

                        // Atualizar o id anterior para o atual
                        previousId = currentId;
                    }
                }

                Console.WriteLine($"Arquivo de índice criado com sucesso, finalizado em {stopwatch.Elapsed.Subtract(timeElapsedTotal).Seconds}s");

                timeElapsedTotal = stopwatch.Elapsed;
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
                int indiceEncontrado = PesquisaBinariaArquivo($"{_basePath}\\{fileName}", productId);

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
                int indiceEncontrado = PesquisaBinariaArquivo($"{_basePath}\\{fileName}", userId);

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
        static (string productId, string end) LerRegistro(FileStream fs, long posicao)
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
        static int PesquisaBinariaArquivo(string caminho, string id)
        {
            // Abrir o arquivo em modo de leitura
            using (FileStream fs = new FileStream(caminho, FileMode.Open, FileAccess.Read))
            {
                long tamanhoRegistro = 27;  // Cada registro tem 25 bytes
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
                        return (int)meio;  // ProductId encontrado, retorna o índice do registro
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

        public void ReorganizeDataFiles(string mainFilePath, string auxFilePath, string outputFilePath, string indexField)
        {
            using var mainFileStream = new FileStream($"{_basePath}\\{mainFilePath}", FileMode.Open);
            using var mainReader = new BinaryReader(mainFileStream);

            FileStream auxFileStream;
            BinaryReader auxReader;

            if (File.Exists(auxFilePath))
            {
                auxFileStream = new FileStream($"{_basePath}\\{auxFilePath}", FileMode.Open);
                auxReader = new BinaryReader(auxFileStream);
            }
            else
            {
                Console.WriteLine("Arquivo auxiliar de inserções está vazio");
                return;
            }

            using var outputFileStream = new FileStream(outputFilePath, FileMode.Create);
            using var outputWriter = new BinaryWriter(outputFileStream);

            string mainId = string.Empty, auxId = string.Empty;
            string mainAutoIncrement = string.Empty, auxAutoIncrement = string.Empty;
            string mainExclusionFlag = string.Empty, auxExclusionFlag = string.Empty;

            try
            {
                bool mainHasData = TryReadRow(mainReader, indexField, out mainId, out mainAutoIncrement, out mainExclusionFlag);
                bool auxHasData = TryReadRow(auxReader, indexField, out auxId, out auxAutoIncrement, out auxExclusionFlag);

                while (mainHasData || auxHasData)
                {
                    if (!mainHasData)
                    {
                        if (auxExclusionFlag != "1") // Se não estiver marcado para exclusão
                        {
                            WriteRow(outputWriter, auxId, auxAutoIncrement, auxReader);
                        }
                        auxHasData = TryReadRow(auxReader, indexField, out auxId, out auxAutoIncrement, out auxExclusionFlag);
                    }
                    else if (!auxHasData)
                    {
                        if (mainExclusionFlag != "1") // Se não estiver marcado para exclusão
                        {
                            WriteRow(outputWriter, mainId, mainAutoIncrement, mainReader);
                        }
                        mainHasData = TryReadRow(mainReader, indexField, out mainId, out mainAutoIncrement, out mainExclusionFlag);
                    }
                    else
                    {
                        // Comparar IDs para fazer o merge
                        int comparison = CompareIds(mainId, auxId);

                        if (comparison < 0)
                        {
                            if (mainExclusionFlag != "1") // Se não estiver marcado para exclusão
                            {
                                WriteRow(outputWriter, mainId, mainAutoIncrement, mainReader);
                            }
                            mainHasData = TryReadRow(mainReader, indexField, out mainId, out mainAutoIncrement, out mainExclusionFlag);
                        }
                        else if (comparison > 0)
                        {
                            if (auxExclusionFlag != "1") // Se não estiver marcado para exclusão
                            {
                                WriteRow(outputWriter, auxId, auxAutoIncrement, auxReader);
                            }
                            auxHasData = TryReadRow(auxReader, indexField, out auxId, out auxAutoIncrement, out auxExclusionFlag);
                        }
                        else
                        {
                            // IDs iguais - preferimos o aux e ignoramos o main se ambos existirem
                            if (auxExclusionFlag != "1") // Se não estiver marcado para exclusão
                            {
                                WriteRow(outputWriter, auxId, auxAutoIncrement, auxReader);
                            }

                            // Pula o main
                            mainHasData = TryReadRow(mainReader, indexField, out mainId, out mainAutoIncrement, out mainExclusionFlag);
                            auxHasData = TryReadRow(auxReader, indexField, out auxId, out auxAutoIncrement, out auxExclusionFlag);
                        }
                    }
                }

                Console.WriteLine("Reordenação concluída com sucesso.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro durante a reorganização: {ex.Message}");
            }
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

        private int CompareIds(string id1, string id2)
        {
            long id1Long = long.Parse(id1);
            long id2Long = long.Parse(id2);

            return id1Long.CompareTo(id2Long);
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

        public List<ProductData> LoadProductAuxData(string auxFilePath)
        {
            var products = new List<ProductData>();

            using (var fileStream = new FileStream(auxFilePath, FileMode.Open))
            using (var reader = new BinaryReader(fileStream))
            {
                while (fileStream.Position < fileStream.Length)
                {
                    string productId = new string(reader.ReadChars(10)).Trim();
                    string categoryId = new string(reader.ReadChars(20)).Trim();
                    string brand = new string(reader.ReadChars(25)).Trim();

                    products.Add(new ProductData
                    {
                        productId = productId,
                        categoryId = categoryId,
                        brand = brand
                    });
                }
            }
            products.OrderBy(p => long.Parse(p.productId));

            return products;
        }

        public List<UserData> LoadUserAuxData(string auxFilePath)
        {
            var users = new List<UserData>();

            using (var fileStream = new FileStream(auxFilePath, FileMode.Open))
            using (var reader = new BinaryReader(fileStream))
            {
                while (fileStream.Position < fileStream.Length)
                {
                    string userId = new string(reader.ReadChars(10)).Trim();
                    string userSession = new string(reader.ReadChars(35)).Trim();
                    string eventType = new string(reader.ReadChars(10)).Trim();

                    users.Add(new UserData
                    {
                        userId = userId,
                        userSession = userSession,
                        eventType = eventType
                    });
                }
            }
            users.OrderBy(u => long.Parse(u.userId));
            return users;
        }


    }
}
