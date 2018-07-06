import re,jieba,sqlite3,torch
from db_api import Blog
import numpy as np
from config import get_device


device = get_device()

DB_FILE = "pixnet.db"

def trim_illegal_char(text):
    res = re.findall(r'[\u4e00-\u9fffa-zA-Z0-9 \t]+',text)
    trimmed = "".join(res)
    return trimmed

def preprocessing_blog_text(text):
    seg_list = jieba.cut( trim_illegal_char(text),cut_all=False)
    s = " ".join(seg_list)
    return s

    
class Example():
    def __init__(self,blog,comment,label):
        self.blog = blog
        self.comment = comment
        self.label = label
    
    def to_dataline(self):
        #assert self.blog.blog_id == self.comment.blog_id

        label = self.label

        blog_id = self.blog.blog_id
        blog_title = preprocessing_blog_text(self.blog.title)
        
        
        comment_id = self.comment.comment_id 
        comment_body = preprocessing_blog_text(self.comment.body)

        line = '%d,%d,%d,%s,%s'%(label,blog_id,comment_id,blog_title,comment_body)
        return line




class DatasetGenerator():
    def __init__(self,conn):
        self.conn = conn


    # 產生 大約 example num筆的example 一個example是一行
    def generate_examples(self,save_path,example_num=50000):
        example_cnt = 0
        current_blog_id = 1
        
        all_examples  = []

        while example_cnt < example_num:
            print('current examples number is %d'%(example_cnt))
            generated_examples = self.generate_examples_of_blog(current_blog_id)
            
            all_examples.extend(generated_examples)
            
            current_blog_id+=1
            example_cnt +=len(generated_examples)


        print('final blog id is %d'%(current_blog_id))
        print('Generate %d examples'%(len(all_examples)))
        
        print('write to file %s'%(save_path))
        with open(save_path,"w",encoding="utf-8") as f:
            for i,example in enumerate(all_examples):
                if (i+1) % 100 == 0:
                    print('save %d line to file %s'%(i,save_path))
                line = example.to_dataline()
                f.write(line+"\n")
        print('save complete')
        return all_examples

    


    def generate_examples_of_blog(self,blog_id):
        blog = Blog.getFromDB(blog_id, conn = self.conn)

        positive_comments = blog.getThisComments()
        positive_num = len(positive_comments)
        negative_num = positive_num
        if positive_num == 0:
            return []
        
        negative_comments = blog.getOtherComments(retreiveCount=negative_num)


        pos_examples = [ Example(blog,comment,1) for comment in positive_comments ]
        neg_exmaples = [ Example(blog,comment,0) for comment in negative_comments ]

        return pos_examples+neg_exmaples

class Vocab():
    def __init__(self,split_token=" "):
        self.w2id = {}
        self.id2w = []
        self.add_word('<PAD>')
        self.add_word('<UNK>')
        self.split_token = split_token
    
    def add_wordlist(self,word_list):
        for word in word_list:
            self.add_word(word)

    def add_word(self,word):
        if word in self.w2id:
            return
        self.id2w.append(word)
        self.w2id[word] = len(self.id2w)-1
    
    def encode(self,text):
        words = text.split(self.split_token)
        tokens = [self.w2id[word] if word in self.w2id  else self.w2id['<UNK>'] for word in words]
        return tokens
    
    def decode(self,tokens):
        return [self.id2w[tid] for tid in tokens]

    def size(self):
        return len(self.id2w)

class Sentence():

    def __init__(self,text,vocab):
        self.text = text
        self.vocab = vocab
        self.tokens = self.__tokenize(self.text)
    
    def __tokenize(self,text):
        tokens = self.vocab.encode(text)
        return tokens

    def length(self):
        return len(self.tokens)

    def to_tensor(self):
        return torch.tensor(self.tokens,device=device)

    def __repr__(self):
        return "Sentence : %s \n %s\n"%(self.text,str(self.tokens))


class BatchSentence():
    def __init__(self,sentences,batch_size):
        self.batch_size = batch_size
        self.sentences = sentences
        self.indexer = BatchIndexer(len(self.sentences),batch_size)


        #self.buffer = {}

    def next_batch(self):
        intv = self.indexer.next_batch_interval()
        if intv is None:
            return None  
        else:
            start_idx,end_idx = intv
            sentences_tensors = [sentence.to_tensor()  for sentence in self.sentences[start_idx:end_idx]]
            sentences_2dtensors,len_2dtensor = self.pad_sentences(sentences_tensors)
            return sentences_2dtensors,len_2dtensor

    def rewind(self):
        self.indexer.rewind()
 
    # tensors list of 1d tensors
    def pad_sentences(self,tensors):
        lens = torch.stack([torch.tensor(tensor.size(0)) for tensor in tensors])
        sorted_lens,sorted_idx = torch.sort(lens,descending=True)
        max_len = torch.max(lens)
        padded_tensor = torch.zeros(lens.size(0),max_len,device=device,dtype=torch.int64)
        for i,(t,l) in enumerate(zip(tensors,lens)):
            padded_tensor[i,0:l] = t
        sorted_padded_tensor = padded_tensor[sorted_idx]
        return sorted_padded_tensor,sorted_lens

class BatchX():
    def __init__(self,batch_title,batch_comment): 
        self.batch_title = batch_title
        self.batch_comment = batch_comment

    def next_batch(self):
        x1 = self.batch_title.next_batch()
        if x1 is None:
            assert self.batch_comment.next_batch() is None
            return None
        return x1,self.batch_comment.next_batch()

    def rewind(self):
        self.batch_title.rewind()
        self.batch_comment.rewind()
    
class BatchLabel():
    def __init__(self,labels,batch_size):
        self.batch_size = batch_size
        self.labels = labels
        self.indexer = BatchIndexer(len(self.labels),batch_size)

    def next_batch(self):
        intv = self.indexer.next_batch_interval()
        if intv is None:
            return None
        else:
            start_idx,end_idx = intv
            tensor = torch.tensor(self.labels[start_idx:end_idx],device=device)
            return tensor

    def rewind(self):
        self.indexer.rewind()

class BatchIndexer():
    def __init__(self,N,batch_size):
        self.batch_size = batch_size
        self.current_idx = 0
        self.N = N

    def rewind(self):
        self.current_idx = 0

    def next_batch_interval(self):
        if self.current_idx == -1:
            return None
        
        tmp_current_idx = self.current_idx
        end_idx = self.current_idx+self.batch_size
        
        if end_idx >=  self.N :
            end_idx = self.N
            self.current_idx = -1
        else:
            self.current_idx = end_idx

        return tmp_current_idx,end_idx

class DataLoader():
    # vocab paramemter:  context --> using training set vocab for test data
    def __init__(self,vocab=None):
        self.vocab = vocab

        self.raw_datas =None
        self.preprocessed_datas = []
        self.batch_data = None
    

    def load_all(self,path,batch_size):
        self.load_raw_data(path)
        self.preprocessing()
        self.load_batches(batch_size)
    
    def load_raw_data(self,path):
        datas = []
        with open(path,"r",encoding="utf-8") as f:
            for line in f.readlines():
                line = line.rstrip()
                label,blog_id,comment_id,blog_title,comment_body = line.split(",")
                label,blog_id,comment_id =  int(label),int(blog_id),int(comment_id)
                datas.append((label,blog_id,comment_id,blog_title,comment_body))

        if self.vocab is None:
            vocab = Vocab()
            for label,blog_id,comment_id,blog_title,comment_body in datas:
                vocab.add_wordlist(blog_title.split(" "))
                vocab.add_wordlist(comment_body.split(" "))  
            self.vocab =vocab
        
        self.raw_datas = datas

    def preprocessing(self):
        for label,blog_id,comment_id,title,comment in self.raw_datas:
            self.preprocessed_datas.append((label,Sentence(title,self.vocab),Sentence(comment,self.vocab)))
    
    def load_batches(self,batch_size):
        labels,titles,comments = tuple(zip(*self.preprocessed_datas ))
        batch_labels,batch_title,batch_comment = BatchLabel(labels,batch_size),BatchSentence(titles,batch_size),BatchSentence(comments,batch_size)
        self.batch_data =  batch_labels,batch_title,batch_comment


 

#connection = sqlite3.connect(DB_FILE)
#maker = DatasetGenerator(connection)
#maker.generate_examples('./train.txt',1000000)
