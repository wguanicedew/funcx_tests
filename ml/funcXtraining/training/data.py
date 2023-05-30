import numpy as np
import re
# from pdb import set_trace

def preprocessing(X_train, kin, name=None, reverse=False, input_file=None):
    if not reverse: # train
        if name is None:
            X_train /= kin
        elif name == 'neglog10plus1':
            X_train = - np.log10((X_train + 1) / kin)
        elif re.compile("^log10.([0-9.]+)+$").match(name): # log10.x
            from common import split_energy, get_energies
            X_train = np.log10((X_train / kin) + 1)
            _, xtrain_list = split_energy(input_file, X_train)
            _, kin_list = split_energy(input_file, kin)
            high = float(re.compile("^log10.([0-9.]+)+$").match(name).groups()[0])
            print('scale to', high)
            scale = []
            for k, v in zip(kin_list, xtrain_list):
                if high/np.sort(v.flatten())[-3] < 1:
                    scale.append((k[0].item(), float(high/np.sort(v.flatten())[-3])))
                else:
                    scale.append((k[0].item(), int(high/np.sort(v.flatten())[-3])))
            scale = dict(scale)
            for k,s in scale.items():
                mask = (kin == k)
                X_train[mask.flatten(), :] *= s
            return X_train, scale
        elif re.compile("^scale.([0-9.]+)+$").match(name): # scale.x
            from common import split_energy, get_energies
            X_train /= kin
            _, xtrain_list = split_energy(input_file, X_train)
            _, kin_list = split_energy(input_file, kin)
            high = float(re.compile("^scale.([0-9.]+)+$").match(name).groups()[0])
            print('scale to', high)
            scale = []
            for k, v in zip(kin_list, xtrain_list):
                if high/np.sort(v.flatten())[-3] < 1:
                    scale.append((k[0].item(), float(high/np.sort(v.flatten())[-3])))
                else:
                    scale.append((k[0].item(), int(high/np.sort(v.flatten())[-3])))
            scale = dict(scale)
            for k,s in scale.items():
                mask = (kin == k)
                X_train[mask.flatten(), :] *= s
            return X_train, scale
        elif re.compile("^slope.([0-9.]+)+$").match(name): # slope.x
            from common import split_energy, get_energies
            X_train /= kin
            _, xtrain_list = split_energy(input_file, X_train)
            _, kin_list = split_energy(input_file, kin)
            high = float(re.compile("^slope.([0-9.]+)+$").match(name).groups()[0])
            scale = []
            scale_list = [-10.0] * 15 
            assert(len(scale_list) >= len(kin_list))
            for k, v, s in zip(kin_list, xtrain_list, scale_list):
                if s < 0:
                    scale.append((k[0].item(), -s))
                else:
                    scale.append((k[0].item(), float(s/np.sort(v.flatten())[-3])))
            scale = dict(scale)
            for k,s in scale.items():
                mask = (kin == k)
                X_train[mask.flatten(), :] *= s
            return X_train, scale
        else:
            raise NotImplementedError
    else: # evaluate
        if name is None:
            X_train *= kin
        elif name == 'neglog10plus1':
             X_train = np.power(10, -X_train) * kin - 1
        elif re.compile("^log10.([0-9.]+)+$").match(name): # log10.x
            import json, tensorflow as tf
            with open(input_file, 'r') as fp:
                scale = json.load(fp)
            scale = dict([(float(k), v) for k,v in scale.items()])
            X_train = X_train.numpy()
            for k,s in scale.items():
                mask = (kin == k)
                X_train[mask.flatten(), :] /= s
            X_train = (np.power(10, X_train) - 1) * kin
            X_train = tf.convert_to_tensor(X_train)
        elif (re.compile("^scale.([0-9.]+)+$").match(name) or \
                re.compile("^slope.([0-9.]+)+$").match(name)
            ): # scale.x
            import json, tensorflow as tf
            with open(input_file, 'r') as fp:
                scale = json.load(fp)
            scale = dict([(float(k), v) for k,v in scale.items()])
            X_train = X_train.numpy()
            for k,s in scale.items():
                mask = (kin == k)
                X_train[mask.flatten(), :] /= s
            X_train *= kin
            X_train = tf.convert_to_tensor(X_train)
        else:
            raise NotImplementedError
    return X_train

