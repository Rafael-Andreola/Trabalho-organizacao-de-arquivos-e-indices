namespace Trabalho1_OrganizaçõesDeArquivosE_Indices.Class
{
    public class BPlusTreeNode
    {
        public bool IsLeaf { get; set; } // Indica se o nó é uma folha
        public List<long> Keys { get; set; } // Chaves no nó
        public List<long> Addresses { get; set; } // Endereços (somente folhas)
        public List<BPlusTreeNode> Children { get; set; } // Filhos (somente nós internos)
        public BPlusTreeNode Next { get; set; } // Ponteiro para o próximo nó (somente folhas)

        public BPlusTreeNode(bool isLeaf)
        {
            IsLeaf = isLeaf;
            Keys = new List<long>();
            Addresses = isLeaf ? new List<long>() : null;
            Children = isLeaf ? null : new List<BPlusTreeNode>();
            Next = null;
        }
    }
}
