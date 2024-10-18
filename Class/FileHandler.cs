using CsvHelper;
using System;
using System.Collections.Generic;
using System.Globalization;
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
    }
}
