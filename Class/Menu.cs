using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Trabalho1_OrganizaçõesDeArquivosE_Indices.Class
{
    public class Menu
    {
        public FileHandler fileHandler;

        private readonly string _fileName; 
        public Menu() 
        { 
            fileHandler = new FileHandler(GetBasePath());

            _fileName = "2019-Oct.csv";
        }
        public void ShowMenu()
        {
            while (true)
            {
                Console.WriteLine("==== MENU ====");
                Console.WriteLine("1. Cria arquivos .bin");
                Console.WriteLine("2. Cria Indice produto");
                Console.WriteLine("4. Mostra o arquivo de produtos");
                Console.WriteLine("5. Executa merge");
                Console.WriteLine("0. Sair");
                Console.Write("Escolha uma opção: ");

                string choice = Console.ReadLine();

                switch(choice)
                {
                    case "1":
                        fileHandler.CreateProductData(fileHandler.ProcessAndSaveSortedBlocks(_fileName, "product"));
                        fileHandler.CreateUserData(fileHandler.ProcessAndSaveSortedBlocks(_fileName, "user"));
                        Console.WriteLine("Dados do produto gravados no arquivo binário.");
                        break;

                    case "2":
                        fileHandler.CreateIndexes();
                        break;

                    case "3":
                        fileHandler.SortAndSaveProductDataBinaryFile("productData");
                        break;

                    case "4":
                        fileHandler.showProductDataBinaryFile("Product");
                        break;

                    case "5":
                        //TODO: Arrumar GetBinaryReaders
                        //fileHandler.CreateDataProduct(fileHandler.GetBinaryReaders(9));
                        break;

                    case "0":
                        Console.WriteLine("Saindo...");
                        return;
                }
            }
        }

        private static string GetBasePath()
        {
            return Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.FullName + "\\Data";
        }
    }
}
