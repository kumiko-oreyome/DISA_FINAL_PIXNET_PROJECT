from data import DataLoader,BatchX
from match import PIXNETNET
import pickle as pkl
from train import  Trainer,loss_function
from torch import optim
from evaluate import Evaluator
import os,argparse
import torch
# load configuration
# load data
# build model
# train
# evaluate
# save restore


def parse():
     parser = argparse.ArgumentParser(description='PIXNET...')


     parser.add_argument('mode',default="test")
     parser.add_argument('-bs', '--batch_size', help='generate data',default=64,type=int)
     parser.add_argument('-en', '--epoch_num', help='generate data',default=100,type=int)
     parser.add_argument('-cpp', '--checkpoint_path', help='generate data',default=None)
     
     parser.add_argument('-lr', '--lr',default=None)

     # for decoding
     parser.add_argument('-r','--save_root',default='./tmp')

     #training path
     parser.add_argument('-tp','--train_path',default='20.txt')
     parser.add_argument('-vp','--val_path',default='20.txt')
     parser.add_argument('-ep','--eval_path',default='20.txt')

     parser.add_argument('-voc','--voc_path',default=None)
    
     args = parser.parse_args()
     return args
print('start')
arg = parse()
print('mode is %s'%(arg.mode))

batch_size = arg.batch_size
epoch_num = arg.epoch_num
save_root_dir = arg.save_root
if not os.path.exists(save_root_dir):
    os.makedirs(save_root_dir)

#if arg.mode == 'test':
#    assert arg.voc_path is not None

vocab = None
if arg.voc_path is not None:
    with open(arg.voc_path,'rb') as f:
        vocab = pkl.load(f)

if arg.mode == 'train':
    train_loader = DataLoader(vocab)
    train_loader.load_all(arg.train_path,batch_size)
    train_batch = train_loader.batch_data 
    vocab = train_loader.vocab
    val_loader = DataLoader(vocab)
    val_loader.load_all(arg.val_path,batch_size)
    val_batch = val_loader.batch_data 
#elif arg.mode == 'test':
#    eval_loader = DataLoader(vocab)
#    eval_loader.load_all(arg.eval_path,batch_size)
#    eval_batch = eval_loader.batch_data 
#    vocab = eval_loader.vocab

embedding_dim,title_gru_units,response_gru_units = 200,128,256
hyper_parameters = (vocab,embedding_dim,title_gru_units,\
                 response_gru_units) 

if arg.checkpoint_path is not None:
    checkpoint = torch.load(arg.checkpoint_path)
    if 'model_hyper' in checkpoint:
        hyper_parameters = checkpoint['model_hyper']
    net =  PIXNETNET(*hyper_parameters)
    net.load_state_dict(checkpoint['model'])
    optimizer = optim.Adam(net.parameters())
    optimizer.load_state_dict(checkpoint['optimzer'])
else:
    net =  PIXNETNET(*hyper_parameters)
    if arg.lr is not None:
        optimizer = optim.Adam(net.parameters(),lr=arg.lr)
    else:
        optimizer = optim.Adam(net.parameters())

def get_X_y(batch_data):
    batch_label,batch_title,batch_comment = batch_data
    X = BatchX(batch_title,batch_comment)
    y = batch_label
    return X,y

if arg.mode == 'train':
    train_X,train_y = get_X_y(train_batch)
    val_X,val_y = get_X_y(val_batch)

    trainer = Trainer(net,optimizer)
    trainer.train(epoch_num,train_X,train_y ,val_X,val_y,save_every = 10,\
              save_dir='%s/model'%(save_root_dir),evaluate_every=10)
#elif arg.mode == 'test':
#    print('evaluate form %s'%(arg.eval_path))
#    eva_X,eva_y = get_X_y(eval_batch)
#    evaluator = Evaluator(net)
#    loss,accu = evaluator.evaluate(eva_X,eva_y,loss_function)
#    print('loss,accu --> (%.3f %.3f)'%(loss,accu))
