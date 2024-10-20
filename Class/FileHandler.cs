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
                    writerProduct.Write(0);
                    writerProduct.Write("\n");

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

                    // Grava o produto no arquivo final
                    writerUser.Write((j++).ToString().PadRight(15).AsSpan(0, 15));
                    writerUser.Write(rowDataUser.userId.PadRight(10).AsSpan(0, 10));
                    writerUser.Write(rowDataUser.userSession.PadRight(35).AsSpan(0, 35));
                    writerUser.Write(rowDataUser.eventType.PadRight(10).AsSpan(0, 10));
                    writerUser.Write(0);
                    writerUser.Write("\n");

                    priorityQueueUser.Remove(firstUser);

                    // Lê o próximo produto do arquivo correspondente
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
        public void showProductDataBinaryFile(string binaryFilePath)
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

                    reader.BaseStream.Seek(48, SeekOrigin.Current); // Pula os outros campos

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

    }
}
