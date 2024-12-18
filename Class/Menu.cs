﻿namespace Trabalho1_OrganizaçõesDeArquivosE_Indices.Class
{
    public class Menu
    {
        public FileHandler fileHandler;
        Dictionary<long, List<long>> hashTable = null;

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
                Console.WriteLine("2. Criar Indices");
                Console.WriteLine("3. Mostra o arquivo de produtos");
                Console.WriteLine("4. Mostra o arquivo de user");
                Console.WriteLine("5. Qual produto mais procurado?");
                Console.WriteLine("6. Qual usuario com mais endereço?");
                Console.WriteLine("7. Pesquisa binária por Id do produto?");
                Console.WriteLine("8. Pesquisa binária por Id do cliente?");
                Console.WriteLine("9. Inserções");
                Console.WriteLine("10. Reordenar arquivos bin");
                Console.WriteLine("11. Deletar indice arquivo de dados Product");
                Console.WriteLine("12. Deletar indice arquivo de dados User");
                Console.WriteLine("13. Menu Hash");
                Console.WriteLine("14. Menu Arvore B+");
                Console.WriteLine("0. Sair");
                Console.Write("Escolha uma opção: ");

                string choice = Console.ReadLine();

                switch (choice)
                {
                    case "1":
                        fileHandler.CreateProductData(fileHandler.ProcessAndSaveSortedBlocks(_fileName, "product"));
                        fileHandler.CreateUserData(fileHandler.ProcessAndSaveSortedBlocks(_fileName, "user"));
                        Console.WriteLine("Dados gravados no arquivo binário.");
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
                        fileHandler.FindProductId(GetId("Digite o ID do produto que deseja procurar:"), "IndexProductId.bin");
                        break;

                    case "8":
                        fileHandler.FindUserId(GetId("Digite o ID do usuario que deseja procurar:"), "IndexUserId.bin");
                        break;

                    case "9":
                        ShowMenuInsert();
                        break;

                    case "10":
                        fileHandler.CreateProductData(fileHandler.ProcessAndSaveSortedBlocksFromBinary("Product.bin", "productToInsert.bin", "product"));
                        fileHandler.CreateUserData(fileHandler.ProcessAndSaveSortedBlocksFromBinary("User.bin", "userToInsert.bin", "user"));
                        break;

                    case "11":
                        fileHandler.DeleteByProductId("Product.bin", GetId("Digite o ID que deseja deletar:"), 80);
                        break;

                    case "12":
                        fileHandler.DeleteByUserId("User.bin", GetId("Digite o ID que deseja deletar:"), 80);
                        break;

                    case "13":
                        ShowHashMenu();
                        break;

                    case "14":
                        ShowBplusMenu();
                        break;

                    case "0":
                        Console.WriteLine("Saindo...");
                        return;
                }
            }
        }

        private void ShowBplusMenu()
        {
            BPlusTree btree = new BPlusTree(4);

            while (true)
            {
                Console.WriteLine("==== MENU ARVORE B+ ====");
                Console.WriteLine("1. Criar indice");
                Console.WriteLine("2. Consultar");
                Console.WriteLine("0. Voltar ao menu principal");
                Console.Write("Escolha uma opção: ");

                string option = Console.ReadLine();

                switch (option)
                {
                    case "1":
                        Console.WriteLine("Criando B+ tree em memória...");
                        btree.InsertByArchive(new FileStream($"{GetBasePath()}\\IndexProductId.bin", FileMode.Open));
                        break;

                    case "2":
                        Console.WriteLine("Digite a chave que deseja pesquisar:");
                        if (!long.TryParse(Console.ReadLine(), out long key))
                        {
                            Console.WriteLine("Chave inválida, tente novamente.");
                            continue;
                        }

                        var list = btree.Search(key);

                        if (list.Count > 0)
                        {
                            foreach (var item in list)
                            {
                                Console.WriteLine("Encontrado no indice: " + item.ToString());
                            }
                        }
                        else
                        {
                            Console.WriteLine("Nenhum indice encontrado para a chave: " + key.ToString());
                        }



                        break;

                    case "0":
                        return;

                    default:
                        Console.WriteLine("Opção inválida, tente novamente.");
                        break;
                }
            }
        }

        private void ShowMenuInsert()
        {
            int option;
            while (true)
            {
                Console.Clear();
                Console.WriteLine("1.Inserir produto\n2.Inserir usuario\n0.Sair\n");
                option = Convert.ToInt32(Console.ReadLine());


                switch (option)
                {
                    case 1:
                        fileHandler.InsertProductIntoAuxFile("productToInsert.bin", newProduct());
                        break;

                    case 2:
                        fileHandler.InsertUserIntoAuxFile("userToInsert.bin", NewUser());
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
            user.userSession = Console.ReadLine();

            Console.WriteLine("Tipo de evento:");
            user.eventType = Console.ReadLine();

            return user;
        }

        public void ShowHashMenu()
        {
            while (true)
            {
                Console.WriteLine("==== MENU HASH ====");
                Console.WriteLine("1. Criar Tabela Hash para UserId");
                Console.WriteLine("2. Consultar na Tabela Hash");
                Console.WriteLine("0. Voltar ao menu principal");
                Console.Write("Escolha uma opção: ");

                string option = Console.ReadLine();

                switch (option)
                {
                    case "1":
                        Console.WriteLine("Criando tabela hash em memória...");
                        hashTable = fileHandler.CreateHashTable("User.bin", 80);
                        Console.WriteLine($"Tabela hash criada com {hashTable.Count} entradas.");
                        break;

                    case "2":
                        if (hashTable == null)
                        {
                            Console.WriteLine("A tabela hash ainda não foi criada. Escolha a opção 1 primeiro.");
                        }
                        else
                        {
                            Console.Write("Digite o UserId para consulta: ");

                            if (!long.TryParse(Console.ReadLine(), out long userId))
                            {
                                continue;
                            }

                            var positions = fileHandler.SearchInHashTable(hashTable, userId);

                            if (positions.Count > 0)
                            {
                                Console.WriteLine($"UserId encontrado nas posições: {string.Join(", ", positions)}");
                            }
                        }
                        break;

                    case "0":
                        return;

                    default:
                        Console.WriteLine("Opção inválida, tente novamente.");
                        break;
                }
            }
        }

    }
}
