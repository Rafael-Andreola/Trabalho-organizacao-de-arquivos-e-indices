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

            _fileName = "2019-Nov.csv";
        }
        public void ShowMenu()
        {
            while (true)
            {
                Console.WriteLine("==== MENU ====");
                Console.WriteLine("1. Cria arquivo base produto e arquivo produtos");
                Console.WriteLine("2. Cria arquivo base usuario");
                Console.WriteLine("4. Mostra o arquivo de produtos");
                Console.WriteLine("5. Cria arquivo produtos");
                Console.WriteLine("0. Sair");
                Console.Write("Escolha uma opção: ");

                string choice = Console.ReadLine();

                switch(choice)
                {
                    case "1":
                        fileHandler.MergeSortedBlocksToBinary(fileHandler.ProcessAndSaveSortedBlocks(_fileName), "productData");
                        Console.WriteLine("Dados do produto gravados no arquivo binário.");
                        break;

                    case "2":
                        //fileHandler.ProcessAndSaveToBinaryFileUserData(_fileName, "userData");
                        Console.WriteLine("Dados do usuário gravados no arquivo binário.");
                        break;

                    case "3":
                        fileHandler.SortAndSaveProductDataBinaryFile("productData");
                        break;

                    case "4":
                        fileHandler.showProductDataBinaryFile("productData");
                        break;

                    case "5":
                        fileHandler.MergeSortedBlocksToBinary(fileHandler.GetBinaryReaders(), "productData");
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
