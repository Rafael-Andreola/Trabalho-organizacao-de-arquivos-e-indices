using CsvHelper;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
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
            int bufferSize = 25000;
            var tempFiles = new List<string>();
            var buffer = new List<ProductData>();
            int fileCount = 0;

            //TODO: Com o map ele funciona mas acredito que não é assim que deve ser feito
            Dictionary<int, byte> keyValuePairs = new Dictionary<int, byte>();

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
                    var product = new ProductData
                    {
                        productId = csv.GetField("product_id") ?? string.Empty,
                        categoryId = csv.GetField("category_id") ?? string.Empty,
                        brand = csv.GetField("brand") ?? string.Empty
                    };

                    //TODO: Teste
                    if(keyValuePairs.TryAdd(int.Parse(product.productId), 1))
                    {
                        buffer.Add(product);
                    }


                    //buffer.Add(product);

                    if (buffer.Count >= bufferSize)
                    {
                        // Ordena o buffer
                        var sortedBuffer = buffer.DistinctBy(x => x.productId).OrderBy(p => p.productId).ToList();

                        Console.WriteLine($"File {fileCount} finalizado em {stopwatch.Elapsed.Subtract(timeElapsedTotal).Seconds} s");

                        timeElapsedTotal = stopwatch.Elapsed;

                        // Grava o bloco ordenado em um arquivo temporário
                        string tempFilePath = $"{_basePath}\\temp_{fileCount}.bin";
                        tempFiles.Add(tempFilePath);
                        WriteBufferToBinaryFile(sortedBuffer, tempFilePath);

                        buffer.Clear();
                        fileCount++;
                    }
                }

                stopwatch.Stop();

                Console.WriteLine($"Processo finalizado em {stopwatch.Elapsed}");

                // Grava o último bloco restante
                if (buffer.Count > 0)
                {
                    var sortedBuffer = buffer.OrderBy(p => p.productId).ToList();
                    string tempFilePath = $"{_basePath}\\temp_{fileCount}.bin";
                    tempFiles.Add(tempFilePath);
                    WriteBufferToBinaryFile(sortedBuffer, tempFilePath);
                }
            }

            return tempFiles;
        }

        private void WriteBufferToBinaryFile(List<ProductData> buffer, string filePath)
        {
            using (var fileStream = new FileStream(filePath, FileMode.Create))
            using (var writer = new BinaryWriter(fileStream))
            {
                foreach (var product in buffer)
                {
                    writer.Write(product.productId.PadRight(10).AsSpan(0, 10));
                    writer.Write(product.categoryId.PadRight(20).AsSpan(0, 20));
                    writer.Write(product.brand.PadRight(25).AsSpan(0, 25));
                }
            }
        }

        public void MergeSortedBlocksToBinary(List<string> tempFiles, string outputBinaryFilePath)
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

                using (var outputFileStream = new FileStream($"{_basePath}\\{outputBinaryFilePath}", FileMode.Create))
                using (var writer = new BinaryWriter(outputFileStream))
                {
                    // Utilizar uma fila de prioridade (min-heap) para mesclar os dados
                    var priorityQueue = new SortedDictionary<string, (ProductData product, int readerIndex)>();

                    // Inicializa a fila com o primeiro registro de cada arquivo
                    for (int i = 0; i < readers.Count; i++)
                    {
                        while (true)
                        {
                            if (TryReadProductData(readers[i], out var product))
                            {
                                if(priorityQueue.TryAdd(product.productId, (product, i)) == true)
                                {
                                    break;
                                }
                            }
                        }
                    }

                    // Executa o merge
                    while (priorityQueue.Count > 0)
                    {
                        // Remove o menor elemento da fila
                        var first = priorityQueue.First();
                        var productData = first.Value.product;
                        var readerIndex = first.Value.readerIndex;
                        priorityQueue.Remove(first.Key);

                        // Grava o produto no arquivo final
                        writer.Write(productData.productId.PadRight(10).AsSpan(0, 10));
                        writer.Write(productData.categoryId.PadRight(20).AsSpan(0, 20));
                        writer.Write(productData.brand.PadRight(25).AsSpan(0, 25));
                        writer.Write("\n");

                        // Lê o próximo produto do arquivo correspondente
                        while (true)
                        {
                            if(TryReadProductData(readers[readerIndex], out var nextProduct))
                            {
                                if (nextProduct.productId == "")
                                {
                                    priorityQueue.Remove(first.Key);
                                    break;
                                }

                                if(priorityQueue.TryAdd(nextProduct.productId, (nextProduct, readerIndex)) == true)
                                {
                                    break;
                                }
                            }
                        }
                    }

                    Console.WriteLine("Acabou");
                }
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
            using FileStream file = new FileStream($"{_basePath}\\{binaryFilePath}", FileMode.Open);

            using var reader = new BinaryReader(file);

            reader.BaseStream.Position = 0;

            while (reader.BaseStream.Position < reader.BaseStream.Length) 
            {
                Console.WriteLine(reader.ReadChars(55));

                reader.BaseStream.Seek(reader.BaseStream.Position + 55, SeekOrigin.Begin);
            }
        }

        public List<string> GetBinaryReaders()
        {
            //TODO: fazer certo
            List<string> tempFiles = new List<string>();

            for (int i = 0; i < 14; i++)
            {
                tempFiles.Add($"{_basePath}\\temp_{i}.bin");
            }

            return tempFiles;
        }
    }
}
