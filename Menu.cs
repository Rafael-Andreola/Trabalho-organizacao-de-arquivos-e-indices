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
        public Menu() 
        { 
            fileHandler = new FileHandler(GetBasePath());
        }
        public void ShowMenu()
        {
            while (true)
            {
                Console.WriteLine("==== MENU ====");
                Console.WriteLine("1. Cria arquivo base produto");
                Console.WriteLine("2. Cria arquivo base usuario");
                Console.WriteLine("0. Sair");
                Console.Write("Escolha uma opção: ");

                string choice = Console.ReadLine();

                switch(choice)
                {
                    case "1":
                        fileHandler.ProcessAndSaveToBinaryFileProductData("2019-Nov.csv", "productData");
                        Console.WriteLine("Dados do produto gravados no arquivo binário.");
                        break;

                    case "2":
                        fileHandler.ProcessAndSaveToBinaryFileUserData("2019-Nov.csv", "userData");
                        Console.WriteLine("Dados do usuário gravados no arquivo binário.");
                        break;

                    case "3":
                        fileHandler.SortAndSaveProductDataBinaryFile("productData");
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
