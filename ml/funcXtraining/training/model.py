import json
import logging
import numpy as np
import os, sys, re
import time
from glob import glob
import matplotlib.pyplot as plt
import random

import tensorflow as tf
from tensorflow.keras import layers
from tensorflow.keras import activations
from tensorflow.keras.models import Model
from tensorflow.keras import regularizers
from tensorflow.keras.layers import Wrapper
from functools import partial

# from pdb import set_trace

class WGANGP:
    def __init__(self, job_config, hp_config, logger):
        tf.keras.backend.set_floatx("float32")
        self.loading = job_config.get('loading', None)
        
        self.model = hp_config.get('model', 'BNswish') # default to photon GAN BNswish
        self.G_size = hp_config.get('G_size', 1)
        self.D_size = hp_config.get('D_size', 1)
        self.optimizer = hp_config.get('optimizer', 'adam')
        self.G_lr = hp_config.get('G_lr', 0.0001)
        self.D_lr = hp_config.get('D_lr', 0.0001)
        self.G_beta1 = hp_config.get('G_beta1', 0.5)
        self.D_beta1 = hp_config.get('D_beta1', 0.5)
        self.batchsize = tf.constant(hp_config.get('batchsize', 512), dtype=tf.int32)
        self.datasize = tf.constant(hp_config.get('datasize', 121000), dtype=tf.int32)
        self.dgratio = tf.constant(hp_config.get('dgratio', 5), dtype=tf.int32)
        self.latent_dim = hp_config.get('latent_dim', 50)
        self.lam = hp_config.get('lam', 50)
        self.conditional_dim = hp_config.get('conditional_dim', 2)
        self.generatorLayers = hp_config.get('generatorLayers', [50, 100, 200])
        for i in range(len(self.generatorLayers)):
            self.generatorLayers[i] = int(self.generatorLayers[i] * self.G_size)
        self.latent_dim = int(self.latent_dim * self.G_size)
        self.nvoxels = hp_config.get('nvoxels', 368)
        self.discriminatorLayers = hp_config.get('discriminatorLayers', [self.nvoxels, self.nvoxels, self.nvoxels])
        self.use_bias = hp_config.get('use_bias', True)
        self.random_mean= hp_config.get('latent_mean', 0.5)
        self.random_std = hp_config.get('latent_std', 0.5)

        self.particle = job_config.get('particle', 'photons')
        self.eta_slice = job_config.get('eta_slice', '20_25')
        self.checkpoint_interval = job_config.get('checkpoint_interval', 1000)
        self.output = os.path.join(job_config.get('output', '../output'), hp_config['model'], f'{self.particle}_eta_{self.eta_slice}')
        if self.loading is not None:
            self.output += '_load'
        self.train_folder = os.path.join(self.output, os.path.splitext(os.path.basename(logger))[0])
        self.no_output = ('evaluate' in logger)
        os.makedirs(self.train_folder, exist_ok=True)
        if not self.no_output:
            logging.basicConfig( handlers=[
                logging.StreamHandler(),
                logging.FileHandler(f'{self.train_folder}/{os.path.splitext(os.path.basename(logger))[0]}.log')
                ], level=logging.INFO, format='%(asctime)s %(message)s')
        else:
            logging.basicConfig( handlers=[
                logging.StreamHandler(),
                ], level=logging.INFO, format='%(asctime)s %(message)s')
        self.max_iter = tf.constant(int(job_config.get('max_iter', 1E6)), dtype=tf.int64)
        self.cache = job_config.get('cache', True)
        self.fix_seed = job_config.get('fix_seed', True)
        if self.fix_seed:
            random.seed(11)
            np.random.seed(11)
            tf.random.set_seed(11)
            if self.loading is not None:
                random.seed(22)
                np.random.seed(22)
                tf.random.set_seed(22)

        # Construct D and G models
        self.G = self.make_generator_functional_model()
        self.D = self.make_discriminator_model()
        
        # Optimizer
        if self.optimizer == 'adam':
            self.generator_optimizer = tf.optimizers.Adam(learning_rate=self.G_lr, beta_1=self.G_beta1)
            self.discriminator_optimizer = tf.optimizers.Adam(learning_rate=self.D_lr, beta_1=self.D_beta1)
        elif self.optimizer == 'clr':
            import tensorflow_addons as tfa
            steps_per_epoch = tf.cast(self.datasize // self.batchsize, tf.int64)
            G_clr = tfa.optimizers.CyclicalLearningRate(initial_learning_rate=self.G_lr/2, maximal_learning_rate=self.D_lr*5, scale_fn=lambda x: 1/(2.**(x-1)), step_size=int(2E5)*steps_per_epoch)
            D_clr = tfa.optimizers.CyclicalLearningRate(initial_learning_rate=self.D_lr/2, maximal_learning_rate=self.D_lr*5, scale_fn=lambda x: 1/(2.**(x-1)), step_size=int(2E5)*steps_per_epoch)
            self.generator_optimizer = tf.optimizers.Adam(learning_rate=G_clr)
            self.discriminator_optimizer = tf.optimizers.Adam(learning_rate=D_clr)
            step = np.arange(0, self.max_iter * steps_per_epoch)
            if ('train' in logger):
                self.plot_clr(G_clr, D_clr, step)

        elif self.optimizer == 'radam':
            import tensorflow_addons as tfa
            G_radam = tfa.optimizers.RectifiedAdam(lr=self.G_lr, beta_1=self.G_beta1, total_steps=10000, warmup_proportion=0.1, min_lr=self.G_lr/10)
            D_radam = tfa.optimizers.RectifiedAdam(lr=self.D_lr, beta_1=self.D_beta1, total_steps=10000, warmup_proportion=0.1, min_lr=self.D_lr/10)
            self.generator_optimizer = tfa.optimizers.Lookahead(G_radam, sync_period=6, slow_step_size=0.5)
            self.discriminator_optimizer = tfa.optimizers.Lookahead(D_radam, sync_period=6, slow_step_size=0.5)
        elif self.optimizer == 'adamW':
            import tensorflow_addons as tfa
            self.generator_optimizer = tfa.optimizers.AdamW(weight_decay=1E-4, lr=self.G_lr, beta_1=self.G_beta1)
            self.discriminator_optimizer = tfa.optimizers.AdamW(weight_decay=1E-4, lr=self.D_lr, beta_1=self.D_beta1)
        else:
            print(self.optimizer, 'not implemented')
            raise NotImplementedError

        # Prepare for check pointing
        self.saver = tf.train.Checkpoint(generator_optimizer=self.generator_optimizer, discriminator_optimizer=self.discriminator_optimizer, generator=self.G, discriminator=self.D,)

        if not self.no_output:
            with open(os.path.join(self.train_folder, 'config.json'), 'w') as fp:
                # this is a reference change, need to correct when saving the config; otherwise evaluation step will get a wrong generator size
                for i in range(len(self.generatorLayers)):
                    self.generatorLayers[i] = int(self.generatorLayers[i] / self.G_size)

                json.dump({
                    'job_config': dict(sorted(job_config.items())),
                    'hp_config': dict(sorted(hp_config.items())),
                }, fp, indent=2)
            logging.info('configuration %s, %s', json.dumps(job_config), json.dumps(hp_config))

    def make_generator_functional_model(self):
        noise = layers.Input(shape=(self.latent_dim,), name="Noise")
        condition = layers.Input(shape=(self.conditional_dim,), name="mycond")
        con = layers.concatenate([noise, condition])
        if not self.no_output:
            logging.info('Use model %s', self.model)
        initializer = tf.keras.initializers.he_uniform()
        bias_node = self.use_bias

        if self.model.startswith("GANv1"):
            G = layers.Dense(self.generatorLayers[0], kernel_initializer=tf.keras.initializers.glorot_normal(), bias_initializer="zeros")(con)
            G = layers.ReLU()(G)
            G = layers.Dense(self.generatorLayers[1], kernel_initializer=tf.keras.initializers.glorot_normal(), bias_initializer="zeros")(G)
            G = layers.ReLU()(G)
            G = layers.Dense(self.generatorLayers[2], kernel_initializer=tf.keras.initializers.glorot_normal(), bias_initializer="zeros")(G)
            G = layers.ReLU()(G)
            G = layers.Dense(self.nvoxels, kernel_initializer=tf.keras.initializers.glorot_normal(), bias_initializer="zeros")(G)
            G = layers.ReLU()(G)
        elif self.model == "BNReLU":
            G = layers.Dense(self.generatorLayers[0], kernel_initializer=initializer, bias_initializer="zeros")(con)
            G = layers.BatchNormalization()(G)
            G = layers.ReLU()(G)
            G = layers.Dense(self.generatorLayers[1], kernel_initializer=initializer, bias_initializer="zeros")(G)
            G = layers.BatchNormalization()(G)
            G = layers.ReLU()(G)
            G = layers.Dense(self.generatorLayers[2],use_bias=bias_node,kernel_initializer=initializer,bias_initializer="zeros")(G)
            G = layers.BatchNormalization()(G)
            G = layers.ReLU()(G)
            G = layers.Dense(self.nvoxels,use_bias=bias_node,kernel_initializer=initializer,bias_initializer="zeros")(G)
            G = layers.ReLU()(G)
        elif self.model == "BNswish":
            initializer = tf.keras.initializers.glorot_normal()
            G = layers.Dense(self.generatorLayers[0],use_bias=bias_node,kernel_initializer=initializer,bias_initializer="zeros")(con)
            G = layers.BatchNormalization()(G)
            G = layers.Activation(activations.swish)(G)
            G = layers.Dense(self.generatorLayers[1],use_bias=bias_node,kernel_initializer=initializer,bias_initializer="zeros")(G)
            G = layers.BatchNormalization()(G)
            G = layers.Activation(activations.swish)(G)
            G = layers.Dense(self.generatorLayers[2],use_bias=bias_node,kernel_initializer=initializer,bias_initializer="zeros")(G)
            G = layers.BatchNormalization()(G)
            G = layers.Activation(activations.swish)(G)
            G = layers.Dense(self.nvoxels,use_bias=bias_node,kernel_initializer=initializer,bias_initializer="zeros")(G)
            G = layers.BatchNormalization()(G)
            G = layers.Activation(activations.swish)(G)
        elif self.model == "BNswishHe":
            G = layers.Dense(self.generatorLayers[0],use_bias=bias_node,kernel_initializer=initializer,bias_initializer="zeros")(con)
            G = layers.BatchNormalization()(G)
            G = layers.Activation(activations.swish)(G)
            G = layers.Dense(self.generatorLayers[1],use_bias=bias_node,kernel_initializer=initializer,bias_initializer="zeros")(G)
            G = layers.BatchNormalization()(G)
            G = layers.Activation(activations.swish)(G)
            G = layers.Dense(self.generatorLayers[2],use_bias=bias_node,kernel_initializer=initializer,bias_initializer="zeros")(G)
            G = layers.BatchNormalization()(G)
            G = layers.Activation(activations.swish)(G)
            G = layers.Dense(self.nvoxels,use_bias=bias_node,kernel_initializer=initializer,bias_initializer="zeros")(G)
            G = layers.BatchNormalization()(G)
            G = layers.Activation(activations.swish)(G)
        elif self.model == "BNLeakyReLU":
            G = layers.Dense(self.generatorLayers[0],use_bias=bias_node,kernel_initializer=initializer,bias_initializer="zeros")(con)
            G = layers.BatchNormalization()(G)
            G = layers.LeakyReLU(alpha=0)(G)
            G = layers.Dense(self.generatorLayers[1],use_bias=bias_node,kernel_initializer=initializer,bias_initializer="zeros")(G)
            G = layers.BatchNormalization()(G)
            G = layers.LeakyReLU(alpha=0)(G)
            G = layers.Dense(self.generatorLayers[2],use_bias=bias_node,kernel_initializer=initializer,bias_initializer="zeros")(G)
            G = layers.BatchNormalization()(G)
            G = layers.LeakyReLU(alpha=0)(G)
            G = layers.Dense(self.nvoxels,use_bias=bias_node,kernel_initializer=initializer,bias_initializer="zeros")(G)
            G = layers.BatchNormalization()(G)
            G = layers.LeakyReLU(alpha=0)(G)
        # elif self.model == "bnF":
        #     G = layers.Dense(self.generatorLayers[0],use_bias=bias_node,kernel_initializer=initializer,bias_initializer="zeros")(con)
        #     G = layers.BatchNormalization()(G)
        #     G = layers.LeakyReLU(alpha=0.03)(G)
        #     G = layers.Dense(self.generatorLayers[1],use_bias=bias_node,kernel_initializer=initializer,bias_initializer="zeros")(G)
        #     G = layers.BatchNormalization()(G)
        #     G = layers.LeakyReLU(alpha=0.03)(G)
        #     G = layers.Dense(self.generatorLayers[2],use_bias=bias_node,kernel_initializer=initializer,bias_initializer="zeros")(G)
        #     G = layers.BatchNormalization()(G)
        #     G = layers.LeakyReLU(alpha=0.03)(G)
        #     G = layers.Dense(self.nvoxels,use_bias=bias_node,kernel_initializer=initializer,bias_initializer="zeros")(G)
        #     G = layers.BatchNormalization()(G)
        #     G = layers.Activation(activation='sigmoid')(G)
        elif self.model == "noBN":
            G = layers.Dense(self.generatorLayers[0],use_bias=bias_node,activation="relu",kernel_initializer=initializer,bias_initializer="zeros")(con)
            G = layers.Dense(self.generatorLayers[1],use_bias=bias_node,activation="relu",kernel_initializer=initializer,bias_initializer="zeros")(G)
            G = layers.Dense(self.generatorLayers[2],use_bias=bias_node,activation="relu",kernel_initializer=initializer,bias_initializer="zeros")(G)
            G = layers.Dense(self.nvoxels,use_bias=bias_node,activation="relu",kernel_initializer=initializer,bias_initializer="zeros")(G)
        elif self.model == "SN":
            G = SpectralNorm(layers.Dense(self.generatorLayers[0],use_bias=bias_node,activation="relu",kernel_initializer=initializer,bias_initializer="zeros"))(con)
            G = SpectralNorm(layers.Dense(self.generatorLayers[1],use_bias=bias_node,activation="relu",kernel_initializer=initializer,bias_initializer="zeros"))(G)
            G = SpectralNorm(layers.Dense(self.generatorLayers[2],use_bias=bias_node,activation="relu",kernel_initializer=initializer,bias_initializer="zeros"))(G)
            G = SpectralNorm(layers.Dense(self.nvoxels,use_bias=bias_node,activation="relu",kernel_initializer=initializer,bias_initializer="zeros"))(G)
        else:
            print(self.model, 'not implemented')
            raise NotImplementedError

        generator = Model(inputs=[noise, condition], outputs=G)
        if not self.no_output:
            generator.summary()
            with open(os.path.join(self.train_folder, 'model.txt'), 'w') as fp:
                generator.summary(print_fn=lambda x: fp.write(x + '\n'))
        return generator

    def make_discriminator_model(self):
        if self.model == "GANv1":
            initializer = tf.keras.initializers.glorot_normal()
            bias_node = True
        elif self.model == "GANv1-Tglo":
            initializer = tf.keras.initializers.glorot_normal()
            bias_node = True
        else:
            initializer = tf.keras.initializers.he_uniform()
            bias_node = self.use_bias

        model = tf.keras.Sequential()
        model.add(layers.Dense(int(self.discriminatorLayers[0] * self.D_size), use_bias=bias_node,
                                input_shape=(self.nvoxels + self.conditional_dim,), kernel_initializer=initializer,bias_initializer="zeros")
                )
        model.add(layers.ReLU())
        model.add(layers.Dense(int(self.discriminatorLayers[1] * self.D_size), use_bias=bias_node,
                                input_shape=(int(self.discriminatorLayers[0] * self.D_size),), kernel_initializer=initializer,bias_initializer="zeros")
                )
        model.add(layers.ReLU())
        model.add(layers.Dense(int(self.discriminatorLayers[2] * self.D_size), use_bias=bias_node,
                                input_shape=(int(self.discriminatorLayers[1] * self.D_size),), kernel_initializer=initializer,bias_initializer="zeros")
                )
        model.add(layers.ReLU())
        model.add(layers.Dense(1, use_bias=bias_node,
                                input_shape=(int(self.discriminatorLayers[2] * self.D_size),), kernel_initializer=initializer,bias_initializer="zeros")
            )

        if not self.no_output:
            model.summary()
            with open(os.path.join(self.train_folder, 'model.txt'), 'a') as fp:
                model.summary(print_fn=lambda x: fp.write(x + '\n'))
        return model

    @tf.function
    def gradient_penalty(self, f, x_real, x_fake, cond_label):
        alpha = tf.random.uniform([self.batchsize, 1], minval=0.0, maxval=1.0)

        inter = alpha * x_real + (1 - alpha) * x_fake
        with tf.GradientTape() as t:
            t.watch(inter)
            pred = self.D(tf.concat([inter, cond_label], 1))
        grad = t.gradient(pred, [inter])[0]

        slopes = tf.sqrt(tf.reduce_sum(tf.square(grad), axis=1))
        gp = self.lam * tf.reduce_mean((slopes - 1.0) ** 2)
        return gp

    @tf.function
    def D_loss(self, x_real, cond_label):
        if self.model == "GANv1":
            z = tf.random.uniform([self.batchsize, self.latent_dim],minval=-1,maxval=1,dtype=tf.dtypes.float32,)
            logging.info('latent dist uniform -1, 1')
        elif self.model == "GANv1-Tlatuni":
            z = tf.random.uniform([self.batchsize, self.latent_dim],minval=-1,maxval=1,dtype=tf.dtypes.float32,)
            logging.info('latent dist uniform -1, 1')
        else:
            z = tf.random.normal([self.batchsize, self.latent_dim],mean=self.random_mean,stddev=self.random_std,dtype=tf.dtypes.float32,)
            logging.info(f'latent dist normal mean {self.random_mean} std {self.random_std}')
        x_fake = self.G(inputs=[z, cond_label])
        D_fake = self.D(tf.concat([x_fake, cond_label], 1))
        D_real = self.D(tf.concat([x_real, cond_label], 1))
        D_loss = (tf.reduce_mean(D_fake)- tf.reduce_mean(D_real)+ self.gradient_penalty(f=partial(self.D, training=True),x_real=x_real,x_fake=x_fake,cond_label=cond_label,))
        return D_loss, D_fake

    @tf.function
    def G_loss(self, D_fake):
        G_loss = -tf.reduce_mean(D_fake)
        return G_loss

    def getTrainData_ultimate(self, n_iter):
        true_batchsize = tf.cast(tf.math.multiply(self.batchsize, self.dgratio), tf.int64)
        n_samples = tf.cast(tf.gather(tf.shape(self.X), 0), tf.int64)
        n_batch = tf.cast(tf.math.floordiv(n_samples, true_batchsize), tf.int64)
        n_shuffles = tf.cast(tf.math.ceil(tf.divide(n_iter, n_batch)), tf.int64)
        ds = tf.data.Dataset.from_tensor_slices((self.X, self.Labels))
        ds = ds.shuffle(buffer_size=n_samples).repeat(n_shuffles).batch(true_batchsize, drop_remainder=True).prefetch(4)
        self.ds_iter = iter(ds)
        X_feature_size = tf.gather(tf.shape(self.X), 1)
        Labels_feature_size = tf.gather(tf.shape(self.Labels), 1)
        self.X_batch_shape = tf.stack((self.dgratio, self.batchsize, X_feature_size), axis=0)
        self.Labels_batch_shape = tf.stack((self.dgratio, self.batchsize, Labels_feature_size), axis=0)

    @tf.function
    def train_loop(self, X_trains, cond_labels):
        for i in tf.range(self.dgratio):
            with tf.GradientTape() as disc_tape:
                (D_loss_curr, D_fake) = self.D_loss(tf.gather(X_trains, i), tf.gather(cond_labels, i))
                gradients_of_discriminator = disc_tape.gradient(D_loss_curr, self.D.trainable_variables)
                self.discriminator_optimizer.apply_gradients(zip(gradients_of_discriminator, self.D.trainable_variables))

        last_index = tf.subtract(self.dgratio, 1)
        with tf.GradientTape() as gen_tape:
            # Need to recompute D_fake, otherwise gen_tape doesn't know the history
            (D_loss_curr, D_fake) = self.D_loss(tf.gather(X_trains, last_index), tf.gather(cond_labels, last_index))
            G_loss_curr = self.G_loss(D_fake)
            gradients_of_generator = gen_tape.gradient(G_loss_curr, self.G.trainable_variables)
            self.generator_optimizer.apply_gradients(zip(gradients_of_generator, self.G.trainable_variables))
            return D_loss_curr, G_loss_curr

    def train(self, X_train, label):
        checkpoint_dir = os.path.join(self.output, 'checkpoints')
        logging.info(f'Training size X: {X_train.shape}, label: {label.shape}')
        os.makedirs(checkpoint_dir, exist_ok=True)

        s_time = time.time()
        dur_train_loop = 0
        D_loss_curr, G_loss_curr = 0.0, 0.0
        if self.cache:
            existing_models = glob(checkpoint_dir + "/model*.index")
            existing_models.sort(key=lambda f: int(re.sub('\D', '', f)))
            existing_models = [m[:-6] for m in existing_models] # remove .index suffix from the name
        else:
            existing_models = []
        meta_data = {'Iteration': [], 'Gloss': [], 'Dloss': [], 'time': []}

        self.X = tf.convert_to_tensor(X_train, dtype=tf.float32)
        self.Labels = tf.convert_to_tensor(label, dtype=tf.float32)
        self.getTrainData_ultimate(self.max_iter)

        if self.loading is not None:
            self.saver.restore(self.loading)
            logging.info(f"Load model from {self.loading}")

        for iteration in range(0, self.max_iter + 1):
            if iteration % self.checkpoint_interval == 0:
                if len(existing_models) > 1:
                    with open(os.path.join(self.train_folder, 'result.json'), 'r') as fp:
                        meta_data = json.load(fp)
                    self.saver.restore(existing_models[0])
                    logging.info(f"Iter: {iteration} skip, load {existing_models[0]}")
                    existing_models.remove(existing_models[0])
                else:
                    e_time = time.time()
                    self.saver.save(file_prefix=checkpoint_dir + "/model")
                    save_time = time.time() - e_time
                    with open(os.path.join(self.train_folder, 'result.json'), 'w') as fp:
                        json.dump(meta_data, fp, indent=2)

                    e_time = time.time()
                    time_diff = e_time - s_time
                    s_time = e_time
                    meta_data['Iteration'].append(iteration)
                    meta_data['time'].append(time_diff / self.checkpoint_interval)
                    meta_data['Gloss'].append(float(G_loss_curr))
                    meta_data['Dloss'].append(float(D_loss_curr))

                    logging.info(f"Iter: {iteration}; D loss: {D_loss_curr:.4f}; G_loss: {G_loss_curr:.4f}; TotalTime: {time_diff:.2f}; TrainLoop: {dur_train_loop:.2f}, Save: {save_time:.2}")
                    dur_train_loop = dur_getTrainData_ultimate = 0.0


            X, Labels = self.ds_iter.get_next()
            if len(existing_models) > 1:
                continue
            else:
                X_trains = tf.reshape(X, self.X_batch_shape)
                cond_labels = tf.reshape(Labels, self.Labels_batch_shape)

                train_loop_start = time.time()
                D_loss_curr, G_loss_curr = self.train_loop(X_trains, cond_labels)
                train_loop_stop = time.time()
                dur_train_loop += train_loop_stop - train_loop_start

        self.plot_loss()
        return

    def plot_clr(self, G_clr, D_clr, step):
        fig, ax = plt.subplots()
        lr = G_clr(step)
        ax.plot(step, lr, label='G lr')
        lr = D_clr(step)
        ax.plot(step, lr, ls='--', label='D lr')
        ax.set_xlabel("Steps")
        ax.set_ylabel("Learning Rate")
        ax.grid(True)
        ax.legend(fontsize=10)
        plt.savefig(os.path.join(self.train_folder, 'learningrate.pdf'))

    def plot_loss(self):
        with open(os.path.join(self.train_folder, 'result.json'), 'r') as fp:
            meta_data = json.load(fp)

        fig, ax = plt.subplots()
        ax.plot(meta_data['Iteration'], meta_data['Gloss'], label="Generator")
        ax.plot(meta_data['Iteration'], meta_data['Dloss'], label="Discriminator")
        ax.set_xlabel("Iteration", fontsize=15)
        ax.set_ylabel("Wasserstein Loss", fontsize=15)
        ax.grid(True)
        ax.legend(fontsize=10)
        plt.savefig(os.path.join(self.train_folder, 'loss.pdf'))
        logging.info('Save to %s', os.path.join(self.train_folder, 'loss.pdf'))

    def predict(self, model_i, labels, ischeck=False):
        checkpoint_dir = os.path.join(self.output, 'checkpoints')
        self.saver.restore(f'{checkpoint_dir}/model-{model_i}').expect_partial()
        if ischeck:
            return 0
        z = tf.random.normal([labels.shape[0], self.latent_dim],mean=self.random_mean,stddev=self.random_std,dtype=tf.dtypes.float32,)
        x_fake = self.G(inputs=[z, labels])
        return x_fake


class SpectralNorm(Wrapper):

    def __init__(self, layer, iteration=1, **kwargs):
        super(SpectralNorm, self).__init__(layer, **kwargs)
        self.iteration = iteration

    def build(self, input_shape):
        if not self.layer.built:
            self.layer.build(input_shape)

            if not hasattr(self.layer, 'kernel'):
                raise ValueError('Invalid layer for SpectralNorm.')

            self.w = self.layer.kernel
            self.w_shape = self.w.shape.as_list()
            self.u = self.add_weight(shape=(1, self.w_shape[-1]), initializer=tf.random_normal_initializer(), name='sn_u', trainable=False, dtype=tf.float32)

        super(SpectralNorm, self).build()

    @tf.function
    def call(self, inputs, training=None):
        self._compute_weights(training)
        output = self.layer(inputs)

        return output

    def _compute_weights(self, training):
        iteration = self.iteration
        w_reshaped = tf.reshape(self.w, [-1, self.w_shape[-1]])

        u_hat = tf.identity(self.u)
        v_hat = None

        for _ in range(self.iteration):
            v_ = tf.matmul(u_hat, tf.transpose(w_reshaped))
            v_hat = tf.nn.l2_normalize(v_)

            u_ = tf.matmul(v_hat, w_reshaped)
            u_hat = tf.nn.l2_normalize(u_)

        if training == True: self.u.assign(u_hat)
        sigma = tf.matmul(tf.matmul(v_hat, w_reshaped), tf.transpose(u_hat))
        w_norm = self.w / sigma
        self.layer.kernel = w_norm
        
    def compute_output_shape(self, input_shape):
        return tf.TensorShape(self.layer.compute_output_shape(input_shape).as_list())

