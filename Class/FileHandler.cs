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

        public List<string> ProcessAndSaveSortedBlocks(string csvFilePath)
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

                long nextIndex = 0;

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
                        // Ordena o buffer
                        var sortedBuffer = buffer.OrderBy(p => p.productId).ToList();

                        // Grava o bloco ordenado em um arquivo temporário
                        string tempFilePath = $"{_basePath}\\temp_{fileCount}.bin";

                        tempFiles.Add(tempFilePath);

                        nextIndex = WriteBufferToBinaryFile(sortedBuffer, tempFilePath, nextIndex);

                        Console.WriteLine($"File {fileCount} finalizado em {stopwatch.Elapsed.Subtract(timeElapsedTotal).Seconds} s");

                        timeElapsedTotal = stopwatch.Elapsed;

                        buffer.Clear();
                        fileCount++;
                    }
                }

                // Grava o último bloco restante
                if (buffer.Count > 0)
                {
                    var sortedBuffer = buffer.OrderBy(p => p.productId).ToList();

                    string tempFilePath = $"{_basePath}\\temp_{fileCount}.bin";
                    tempFiles.Add(tempFilePath);
                    WriteBufferToBinaryFile(sortedBuffer, tempFilePath, nextIndex);

                    Console.WriteLine($"File {fileCount} finalizado em {stopwatch.Elapsed.Subtract(timeElapsedTotal).Seconds} s");
                }

                stopwatch.Stop();

                Console.WriteLine($"Processo finalizado em {stopwatch.Elapsed}");
            }

            return tempFiles;
        }

        private long WriteBufferToBinaryFile(List<Row> buffer, string filePath, long nextIndex)
        {
            using (var fileStream = new FileStream(filePath, FileMode.Create))
            using (var writer = new BinaryWriter(fileStream))
            {
                foreach (var row in buffer)
                {
                    //writer.Write(nextIndex++.ToString().PadRight(15).AsSpan(0, 15));
                    writer.Write(row.productId.PadRight(10).AsSpan(0, 10));
                    writer.Write(row.categoryId.PadRight(20).AsSpan(0, 20));
                    writer.Write(row.brand.PadRight(25).AsSpan(0, 25));
                    writer.Write(row.userId.PadRight(15).AsSpan(0, 15));
                    writer.Write(row.userSession.PadRight(35).AsSpan(0, 35));
                    writer.Write(row.eventType.PadRight(10).AsSpan(0, 10));
                }
            }

            return nextIndex;
        }

        public void CreateData(List<string> tempFiles)
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
                using var outputFileUser = new FileStream($"{_basePath}\\User.bin", FileMode.Create);

                using var writerProduct = new BinaryWriter(outputFileProduct);
                using var writerUser = new BinaryWriter(outputFileUser);

                var priorityQueue = new SortedList<string, (Row row, int readerIndex)>();

                var stopwatch = new Stopwatch();
                stopwatch.Start();

                // Inicializa a fila com o primeiro registro de cada arquivo
                for (int i = 0; i < readers.Count; i++)
                {
                    if(TryReadRowData(readers[i], out var row))
                    {
                        priorityQueue.Add(row.productId, (row, i));
                    }
                }

                Console.WriteLine("Iniciando Merge.");

                int j = 0;

                // Executa o merge
                while (priorityQueue.Count > 0)
                {
                    // Remove o menor elemento da fila
                    var first = priorityQueue.First();
                    var rowData = first.Value.row;
                    var readerIndex = first.Value.readerIndex;

                    // Grava o produto no arquivo final
                    writerProduct.Write((j).ToString().PadRight(15).AsSpan(0, 15));
                    writerProduct.Write(rowData.productId.PadRight(10).AsSpan(0, 10));
                    writerProduct.Write(rowData.categoryId.PadRight(20).AsSpan(0, 20));
                    writerProduct.Write(rowData.brand.PadRight(25).AsSpan(0, 25));
                    writerProduct.Write("\n");
                    // Grava o user no arquivo final
                    writerUser.Write((j).ToString().PadRight(15).AsSpan(0,10));
                    writerUser.Write(rowData.userId.PadRight(15).AsSpan(0, 15));
                    writerUser.Write(rowData.userSession.PadRight(35).AsSpan(0, 35));
                    writerUser.Write(rowData.eventType.PadRight(10).AsSpan(0, 10));
                    writerUser.Write("\n");


                    j++;
                    priorityQueue.Remove(first.Key);
                    
                    // Lê o próximo produto do arquivo correspondente
                    if (TryReadRowData(readers[readerIndex], out var row))
                    {
                        if(row == null)
                        {
                            continue;
                        }

                        
                        priorityQueue.Add(row.productId, (row, readerIndex));
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
            }
        }

        private bool TryReadRowData(BinaryReader reader, out Row row)
        {
            try
            {
                // Lê os dados do arquivo em tamanhos fixos

                //string id = new string(reader.ReadChars(15)).Trim();
                string productId = new string(reader.ReadChars(10)).Trim();
                string categoryId = new string(reader.ReadChars(20)).Trim();
                string brand = new string(reader.ReadChars(25)).Trim();
                string userId = new string(reader.ReadChars(15)).Trim();
                string userSession = new string(reader.ReadChars(35)).Trim();
                string eventType = new string(reader.ReadChars(10)).Trim();
                


                // Cria um novo objeto ProductData com os dados lidos
                row = new Row
                {
                    //Id = long.Parse(id),
                    productId = productId,
                    categoryId = categoryId,
                    brand = brand,
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

        private bool TryReadUserData(BinaryReader reader, out UserData user)
        {
            try
            {
                // Lê os dados do arquivo em tamanhos fixos
                string userId = new string(reader.ReadChars(15)).Trim();
                string userSession = new string(reader.ReadChars(30)).Trim();
                string eventType = new string(reader.ReadChars(10)).Trim();

                // Cria um novo objeto ProductData com os dados lidos
                user = new UserData
                {
                    userId = userId,
                    userSession = userSession,
                    eventType = eventType
                };

                return true; // Leitura bem-sucedida
            }
            catch (EndOfStreamException)
            {
                user = null; // Final do arquivo alcançado
                return false;
            }
            catch (Exception ex)
            {
                // Tratamento para outros erros potenciais
                Console.WriteLine($"Erro ao ler os dados: {ex.Message}");
                user = null;
                return false;
            }
        }

        private bool TryReadProductData(BinaryReader reader, out ProductData product)
        {
            try
            {
                // Lê os dados do arquivo em tamanhos fixos
                string productId = new string(reader.ReadChars(10)).Trim();
                string categoryId = new string(reader.ReadChars(20)).Trim();
                string brand = new string(reader.ReadChars(25)).Trim();

                // Cria um novo objeto ProductData com os dados lidos
                product = new ProductData
                {
                    productId = productId,
                    categoryId = categoryId,
                    brand = brand
                };

                return true; // Leitura bem-sucedida
            }
            catch (EndOfStreamException)
            {
                product = null; // Final do arquivo alcançado
                return false;
            }
            catch (Exception ex)
            {
                // Tratamento para outros erros potenciais
                Console.WriteLine($"Erro ao ler os dados: {ex.Message}");
                product = null;
                return false;
            }
        }

/////////////////////////////////////////// Criação sem ordenar

        public void ProcessAndSaveToBinaryFileProductData(string csvFilePath, string binaryFilePath, bool append = false)
        {
            using (var fileStream = new FileStream($"{_basePath}\\{binaryFilePath}", append ? FileMode.Append : FileMode.Create))
            using (var writer = new BinaryWriter(fileStream))
            using (var reader = new StreamReader($"{_basePath}\\{csvFilePath}"))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                csv.Read();
                csv.ReadHeader();

                while (csv.Read())
                {
                    var productId = csv.GetField("product_id") ?? string.Empty;
                    var categoryId = csv.GetField("category_id") ?? string.Empty;
                    var brand = csv.GetField("brand") ?? string.Empty;

                    writer.Write(productId.PadRight(10).AsSpan(0, 10));
                    writer.Write(categoryId.PadRight(20).AsSpan(0, 20));
                    writer.Write(brand.PadRight(25).AsSpan(0, 25));
                    writer.Write("/n");
                }
            }
        }

        public void ProcessAndSaveToBinaryFileUserData(string csvFilePath, string binaryFilePath, bool append = false)
        {
            using (var fileStream = new FileStream($"{_basePath}\\{binaryFilePath}", append ? FileMode.Append : FileMode.Create))
            using (var writer = new BinaryWriter(fileStream))
            using (var reader = new StreamReader($"{_basePath}\\{csvFilePath}"))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                csv.Read();
                csv.ReadHeader();

                while (csv.Read())
                {
                    var userId = csv.GetField("user_id") ?? string.Empty;
                    var sessionId = csv.GetField("user_session") ?? string.Empty;
                    var eventType = csv.GetField("event_type") ?? string.Empty;

                    writer.Write(userId.PadRight(10).AsSpan(0, 10));
                    writer.Write(sessionId.PadRight(10).AsSpan(0, 10));
                    writer.Write(eventType.PadRight(20).AsSpan(0, 20));
                    writer.Write("/n");
                }
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
    }
}
