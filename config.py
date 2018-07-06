import torch
device_name = "cuda" if torch.cuda.is_available() else "cpu"
def get_device():
    #print('device name')
    #print(device_name)
    import torch
    device = torch.device(device_name)
    print(device)
    return device