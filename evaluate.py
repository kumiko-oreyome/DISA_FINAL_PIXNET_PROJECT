import torch
from config import get_device
device = get_device()
class Evaluator():
    def __init__(self,model):
        self.model = model
    
    def evaluate(self,eva_X,eva_y,loss_func,keep_loss=True,keep_accuracy=True):
        loop_cnt = 0
        if keep_loss:
            loss_sum = 0
        if keep_accuracy:
            accu_list = []

        while True:
            X,y = eva_X.next_batch(),eva_y.next_batch()
            if X is None:
                assert y is None
                eva_X.rewind()
                eva_y.rewind()
                break
            probs = self.model(*X)
            if keep_loss:
                loss = loss_func(probs, y)
                loss_sum+=loss.item()
                

            if keep_accuracy:
                predictions = torch.argmax(probs,1) 
                accu = torch.eq( y,predictions).float()
                accu_list.append(accu)

            loop_cnt+=1

        
        ret = []
        if keep_loss :
            avg_loss = loss_sum/loop_cnt
            ret.append(avg_loss)
        if keep_accuracy:
            avg_accu = torch.cat(accu_list).mean()
            ret.append(avg_accu)
        
        return tuple(ret)
    
    #def prediction(self,eva_X,eva_y):
    #    probs = self.model(eva_X)
    #    predictions = torch.argmax(probs)
    #    return predictions
        