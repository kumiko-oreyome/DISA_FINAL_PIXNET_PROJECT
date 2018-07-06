import torch
import torch.nn as nn
from torch.autograd import Variable
import numpy as np
from torch.nn.utils.rnn import pack_padded_sequence,pad_packed_sequence
from config import get_device

device = get_device()
#torch.set_printoptions(edgeitems=20)


def dynamic_rnn(rnn_cell,padded_sequences,seq_lens,h0s):
    #print(seq_lens)
    #seq_lens, perm_idx = seq_lens.sort(0, descending=True)
    #print(perm_idx)
    #padded_sequences =  padded_sequences[perm_idx]
                                                          #seq length: type is not a tensor
    packed_input = pack_padded_sequence(padded_sequences, seq_lens.data.cpu().numpy(),batch_first=True)
    _ , ht = rnn_cell(packed_input,h0s)
    # batch_first ... but output shape of ht is still (num_layer*seq_len,batch,featueres)
    ht = torch.t(ht)
    ht = ht.contiguous() 
    return  ht



class PIXNETNET(nn.Module):
    def __init__(self,vocab,embedding_dim,title_gru_units,\
                 response_gru_units):
        super(PIXNETNET, self).__init__()

        self.embedding_dim = embedding_dim 
        self.vocab = vocab
        self.vocab_size = self.vocab.size()

        self.title_gru_units = title_gru_units
        self.response_gru_units = response_gru_units
        # (title_gru_units+body_gru_units+.response_gru_units) 
        # bidirectional : sum up two direction output vector
        self.prediction_layer_dim = (self.title_gru_units+self.response_gru_units)
        

        self.__build()
    
    def hyper_parameters(self):
        return  self.vocab,self.embedding_dim,self.vocab,self.title_gru_units,\
        self.response_gru_units,self.prediction_layer_dim 

    def __build(self):
        self.embeddings = nn.Embedding(self.vocab_size, self.embedding_dim).to(device)
        
        self.title_gru = nn.GRU(self.embedding_dim, self.title_gru_units,bidirectional=True,batch_first=True).to(device)
        self.response_gru = nn.GRU(self.embedding_dim, self.response_gru_units,bidirectional=True,batch_first=True).to(device)

        self.prediction_layer = nn.Linear(self.prediction_layer_dim,2).to(device)

        nn.init.xavier_normal_(self.embeddings.weight)
        nn.init.xavier_normal_(self.prediction_layer.weight)

        self.init_gru(self.title_gru)
        self.init_gru(self.response_gru)
           
    def init_gru(self,gru,num_layer=1,initializer=nn.init.orthogonal_):
        for attr_list in gru.__dict__['_all_weights']:
            for attr in attr_list:
                if attr.startswith("weight_ih_l") or \
                    attr.startswith("weight_hh_l"):
                    gate = getattr(gru, attr)
                    W_r,W_z,W_n = gate.chunk(3,0)
                    initializer(W_r)
                    initializer(W_z)
                    initializer(W_n)

    def init_gru_state(self,batch_size,hidden_size,layer_num=1,direction_num=2):
        return torch.zeros((direction_num*layer_num,batch_size,hidden_size)).to(device)     

    #    title : title_tensor(N,MAX_TITLE_TOKEN_NUM of this batch), title_token_num(N)
    #    blogs : blogs  (N,MAX_TITLE_TOKEN_NUM),blog_token_num (N)
    #    response : .....
    def forward(self,title,response):
        batch_size = response[0].size(0)
 

        title_embedding = self.embeddings(title[0]) 
        response_embedding = self.embeddings(response[0])


        title_h0 = self.init_gru_state(batch_size,self.title_gru_units)  
        response_h0 = self.init_gru_state(batch_size,self.response_gru_units)


        # last time step output of each sequneces
        title_vector = dynamic_rnn(self.title_gru,title_embedding,title[1],title_h0)
        response_vector = dynamic_rnn(self.response_gru,response_embedding,response[1],response_h0)     
        response_vector = response_vector[:,0,:]+response_vector[:,1,:]
        title_vector = title_vector[:,0,:]+title_vector[:,1,:]
        concated_vector = torch.cat((title_vector,response_vector),dim=1)
        score = self.prediction_layer(concated_vector)
        p = torch.nn.functional.log_softmax(score,1)
        return p






   
