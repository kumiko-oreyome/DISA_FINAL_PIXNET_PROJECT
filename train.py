import torch,os
from tqdm import tqdm
import torch.nn.functional as F
from evaluate import Evaluator
import numpy as np
from itertools import zip_longest

# cross entropy
def loss_function(log_probs,ys):
    #likelihood = probs.log()
    loss = F.nll_loss(log_probs,ys)
    return loss

# because numerically unstable operation
# https://discuss.pytorch.org/t/nan-loss-in-rnn-model/655/2
def  nll(probs,ys):
    loss = F.nll_loss(likelihood,ys)
    return loss

class Trainer():
    def __init__(self,model,optimizer):
        self.model = model
        self.optimizer = optimizer
        self.evaluator = Evaluator(model)
        
    def train(self,epoch_num,train_X,train_y,val_X,val_y,save_every,save_dir,evaluate_every):
       
        for epoch in tqdm(range(epoch_num)):
            loss_history = []
            print('Epoch :%d '%(epoch+1))
            
            while True:
                X,y = train_X.next_batch(),train_y.next_batch()
                if X is None:
                    try:
                        assert y is None
                    except:
                        print(y)
                    train_X.rewind()
                    train_y.rewind()
                    break
                self.model.zero_grad() 
                probs = self.model(*X)
                loss = loss_function(probs,y)
                loss.backward()
                clip = 5.0
                torch.nn.utils.clip_grad_norm_(self.model.parameters(), clip)
                self.optimizer.step()
                loss_history.append(loss.item())
            
            print('avg loss is :%.3f'%(np.array(loss_history).mean())) 

        
            if (epoch+1) % save_every == 0:
                print('save model to %s on epoch %d'%(save_dir,epoch+1))
                if not os.path.exists(save_dir):
                    os.makedirs(save_dir)
                torch.save({'epoch':epoch,'model':self.model.state_dict(),'optimzer':self.optimizer.state_dict(),\
                    'model_hyper':self.model.hyper_parameters()},os.path.join(save_dir,'%d.pkl'%(epoch)))
            
            if  (epoch+1) % evaluate_every == 0:
                print('validation on epoch : %d'%(epoch+1))
                loss,accu = self.evaluator.evaluate(val_X,val_y,loss_function,True,True)
                print('(loss,accu)--> (%.3f,%.3f)'%(loss,accu)) 
            
    #def load(self,model_dir):
    #    checkpoint = torch.load(model_dir)
    #    qnetwork = QNetwork(*checkpoint['model_hyper'])
    #    qnetwork.load_state_dict(checkpoint['model']) 
        
