from omegaconf import OmegaConf


def load_config_file(file_path):
    return OmegaConf.load(file_path)
