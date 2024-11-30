using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Trabalho1_OrganizaçõesDeArquivosE_Indices.Class
{
    public class BPlusTree
    {
        private readonly int _order; // Ordem da árvore
        private BPlusTreeNode _root;

        public BPlusTree(int order)
        {
            _order = order;
            _root = new BPlusTreeNode(true); // Começa com um nó folha
        }

        // Método para inserir uma chave e seu endereço
        public void Insert(long key, long address)
        {
            BPlusTreeNode root = _root;
            if (root.Keys.Count == 2 * _order - 1) // Se o nó raiz estiver cheio
            {
                BPlusTreeNode newRoot = new BPlusTreeNode(false); // Novo nó raiz
                newRoot.Children.Add(root); // O antigo nó raiz se torna filho do novo nó raiz
                SplitChild(newRoot, 0, root); // Divide o antigo nó raiz
                _root = newRoot; // Atualiza a raiz
            }
            InsertNonFull(_root, key, address); // Insere no nó apropriado
        }

        public void InsertByArchive(FileStream file)
        {
            using var reader = new BinaryReader(file);

            var stopwatch = new Stopwatch();

            reader.BaseStream.Position = 0;

            while (reader.BaseStream.Position < reader.BaseStream.Length)
            {
                    long key = long.Parse(reader.ReadChars(10));
                    long address = long.Parse(reader.ReadChars(15));

                    this.Insert(key, address);

                    reader.BaseStream.Position += 2;
            }
            stopwatch.Stop();

            Console.WriteLine(stopwatch.Elapsed.ToString());

        }

        // Insere em um nó não cheio
        private void InsertNonFull(BPlusTreeNode node, long key, long address)
        {
            int i = node.Keys.Count - 1;

            if (node.IsLeaf)
            {
                // Insere a chave e o endereço no nó folha
                while (i >= 0 && key < node.Keys[i])
                    i--;
                node.Keys.Insert(i + 1, key);
                node.Addresses.Insert(i + 1, address);
            }
            else
            {
                // Busca o filho adequado
                while (i >= 0 && key < node.Keys[i])
                    i--;
                i++;
                BPlusTreeNode child = node.Children[i];
                if (child.Keys.Count == 2 * _order - 1) // Se o filho estiver cheio
                {
                    SplitChild(node, i, child);
                    if (key > node.Keys[i])
                        i++;
                }
                InsertNonFull(node.Children[i], key, address);
            }
        }

        // Divide um nó cheio
        private void SplitChild(BPlusTreeNode parent, int index, BPlusTreeNode child)
        {
            int mid = _order - 1; // Posição do meio
            BPlusTreeNode newChild = new BPlusTreeNode(child.IsLeaf);

            // Copia a metade superior das chaves do nó cheio para o novo nó
            for (int i = mid + 1; i < child.Keys.Count; i++)
            {
                newChild.Keys.Add(child.Keys[i]);
            }

            // Remove essas chaves do nó original
            child.Keys.RemoveRange(mid + 1, child.Keys.Count - (mid + 1));

            if (child.IsLeaf)
            {
                // Copia os endereços associados às chaves para o novo nó
                for (int i = mid + 1; i < child.Addresses.Count; i++)
                {
                    newChild.Addresses.Add(child.Addresses[i]);
                }

                // Remove os endereços do nó original
                child.Addresses.RemoveRange(mid + 1, child.Addresses.Count - (mid + 1));

                // Atualiza o ponteiro do próximo nó
                newChild.Next = child.Next;
                child.Next = newChild;
            }
            else
            {
                // Copia os filhos correspondentes para o novo nó (caso não seja folha)
                for (int i = mid + 1; i < child.Children.Count; i++)
                {
                    newChild.Children.Add(child.Children[i]);
                }

                // Remove os filhos do nó original
                child.Children.RemoveRange(mid + 1, child.Children.Count - (mid + 1));
            }

            // Insere a chave do meio no nó pai
            if (index > parent.Keys.Count)
            {
                throw new InvalidOperationException($"Index {index} is invalid for parent with {parent.Keys.Count} keys.");
            }

            parent.Keys.Insert(index, child.Keys[mid]);
            child.Keys.RemoveAt(mid);

            // Adiciona o novo filho ao pai
            parent.Children.Insert(index + 1, newChild);
        }


        // Método de busca
        public List<long> Search(long key)
        {
            return SearchInNode(_root, key);
        }

        private List<long> SearchInNode(BPlusTreeNode node, long key)
        {
            int i = 0;
            while (i < node.Keys.Count && key > node.Keys[i])
                i++;

            if (node.IsLeaf)
            {
                if (i < node.Keys.Count && key == node.Keys[i])
                    return new List<long> { node.Addresses[i] };
                else
                    return new List<long>();
            }
            else
            {
                return SearchInNode(node.Children[i], key);
            }
        }
    }
}
