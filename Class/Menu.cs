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
                Console.WriteLine("3. Mostra o arquivo de produtos");
                Console.WriteLine("4. Mostra o arquivo de user");
                Console.WriteLine("5. Qual produto mais procurado?");
                Console.WriteLine("6. Qual usuario com mais endereço?");
                Console.WriteLine("7. Pesquisa binária por Id do produto?");
                Console.WriteLine("8. Pesquisa binária por Id do cliente?");
                Console.WriteLine("9. Inserções");
                Console.WriteLine("10. Remoções");
                Console.WriteLine("11. Reorganizar");

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
                        fileHandler.CreateIndex("Product.bin", "IndexProductId.bin");
                        fileHandler.CreateIndex("User.bin", "IndexUserId.bin");
                        break;

                    case "3":
                        fileHandler.showDataBinaryFile("Product");
                        break;

                    case "4":
                        fileHandler.showDataBinaryFile("User");
                        break;

                    case "5":
                        fileHandler.ProductWithMoreInteraction("IndexProductId.bin");
                        break;

                    case "6":
                        fileHandler.UserWithMoreInteraction("IndexUserId.bin");
                        break;

                    case "7":
                        fileHandler.FindProductId( GetId("Digite o ID do produto que deseja procurar:") ,"IndexProductId.bin");
                        break;

                    case "8":
                        fileHandler.FindUserId(GetId("Digite o ID do usuario que deseja procurar:"), "IndexUserId.bin");
                        break;
                    case "9":
                        fileHandler.DeleteByProductId("Product.bin", GetId("Digite o ID que deseja deletar:"), 80);
                        break;

                    case "10":
                        fileHandler.DeleteByUserId("User.bin", GetId("Digite o ID que deseja deletar:"), 80);
                        break;

                    case "9":
                        ShowMenuInsert();
                        break;

                    case "10":
                        break;

                    case "11":
                        fileHandler.ReorganizeDataFiles("Product.bin", "productToInsert", "NewProduct.bin", "product");
                        fileHandler.ReorganizeDataFiles("User.bin", "userToInsert", "NewUser.bin", "user");
                        break;

                    case "0":
                        Console.WriteLine("Saindo...");
                        return;
                }
            }
        }

        private void ShowMenuInsert()
        {
            int option;
            while(true)
            {
                Console.Clear();
                Console.WriteLine("1.Inserir produto\n2.Inserir usuario\n0.Sair\n");
                option = Convert.ToInt32(Console.ReadLine());


                switch (option)
                {
                    case 1:
                        fileHandler.InsertProductIntoAuxFile("productToInsert", newProduct());
                        break;

                    case 2:
                        fileHandler.InsertUserIntoAuxFile("userToUser", NewUser());
                        break;

                    case 0:
                        return;
                }
            }
        }

        private static string GetBasePath()
        {
            return Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.FullName + "\\Data";
        }

        private string GetId(string message)
        {
            Console.WriteLine(message);
            return Console.ReadLine();
        }

        private ProductData newProduct()
        {
            var product = new ProductData();

            Console.WriteLine("Id do produto:");
            product.productId = Console.ReadLine();

            Console.WriteLine("Id da categoria do produto:");
            product.categoryId = Console.ReadLine();

            Console.WriteLine("Marca:");
            product.brand = Console.ReadLine();

            return product;
        }

        private UserData NewUser()
        {
            var user = new UserData();

            Console.WriteLine("Id do usuario:");
            user.userId = Console.ReadLine();

            Console.WriteLine("Id da sessão do usuario:");
            user.userSession= Console.ReadLine();

            Console.WriteLine("Tipo de evento:");
            user.eventType = Console.ReadLine();

            return user;
        }
    }
}
