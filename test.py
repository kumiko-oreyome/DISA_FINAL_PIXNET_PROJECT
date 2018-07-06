from data import Vocab,Sentence,BatchSentence,BatchLabel,DataLoader,BatchX
from match import PIXNETNET
from train import Trainer
from torch import optim

class Test():
    def __init__(self):
        self.word_list = ["a","b","c","d","z","y","x","w"]
        self.vocab = Vocab()
        self.vocab.add_wordlist(self.word_list)
        self.dl = DataLoader()
        self.dl.load_all('20.txt',batch_size=2)
        

    def test_sentences(self):
        print('test sentences')
        texts = ["a x d z w w a","a b c d w x y z","w w x x a a"]
        sentences = [ Sentence(text,self.vocab) for text in texts]
        tensors = [sentence.to_tensor() for sentence in sentences]

        print('vocab')
        print(self.vocab.w2id)

        print('encode')
        tokens = [self.vocab.encode(text) for text in texts]
        print(tokens)

        print('decode')
        decoded =  [self.vocab.decode(l) for l  in tokens]
        print(decoded)

        print('sentences')
        print(sentences)
        print('tensors')
        print(tensors)
        return sentences

    def test_batch_sentence(self):
        print('test batch sentnece')
        batch_size = 3
        sentences = self.test_sentences()
        batch = BatchSentence(sentences,batch_size)
        print('Batch')
        print(batch.next_batch())
        print(batch.next_batch())
    
    def test_batch_labels(self):
        print('test batch labels')
        labels = [1,0,1,1,0]
        batch_size = 3
        batch = BatchLabel(labels,batch_size)
        print(batch.next_batch())
        print(batch.next_batch())

    def test_load_data_from_file(self,path):
        pass
        #print('test load data from file')
        #dl = DataLoader()
        #datas,vocab = dl.load(path)
        #print('vocab')
        #print(vocab)
        #print('num data')
        #print(len(datas))
        #print(datas[0:2])
    
    def  test_load_batch_from_file(self):
        
        labels,titles,bodys,comments = tuple(zip(*self.dl.preprocessed_datas ))
       
        vocab = self.dl.vocab
        batch_labels,batch_title,batch_body,batch_comment = self.dl.batch_data
        print('vocab')
        print(vocab.w2id)
        print('batchs')
        print('- '*30)
        print('labels')
        print('b')
        for b in batch_labels:
            print(b)
        print('bb')
        for b in batch_labels:
            print(b)
        print('titles')
        print('t')
        for t in batch_title:
            print(t)
        print('tt')
        for t in batch_title:
            print(t)
        print('body')
        print(next(batch_body))
        print('comment')
        print(next(batch_comment))
        
    
    def test_build_net(self):
        net =  PIXNETNET(self.dl.vocab,embedding_dim=50,title_gru_units=50,body_gru_units=50,\
                 response_gru_units=50)

    def test_forward(self):
        net =  PIXNETNET(self.dl.vocab,embedding_dim=50,title_gru_units=50,body_gru_units=50,\
                 response_gru_units=50)
        _,batch_title,batch_body,batch_comment = self.dl.batch_data
        p  = net.forward(batch_title.next_batch(),batch_body.next_batch(),batch_comment.next_batch())
        print(p)

    def test_train(self):
        save_root_dir = './tmp'
        print('test train')
        net =  PIXNETNET(self.dl.vocab,embedding_dim=50,title_gru_units=50,body_gru_units=50,\
                 response_gru_units=50)
        optimizer = optim.Adam(net.parameters())
        batch_label,batch_title,batch_body,batch_comment = self.dl.batch_data
        X = BatchX(batch_title,batch_body,batch_comment)
        y = batch_label
 
        trainer = Trainer(net,optimizer)

        trainer.train(10,X,y,X,y,save_every = 10,\
              save_dir='%s/model'%(save_root_dir),evaluate_every=10)
              

test = Test()
#test.test_load_batch_from_file()
test.test_train()