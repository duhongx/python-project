"""配置加载模块"""

import os
import yaml
from pathlib import Path

DEFAULT_CONFIG_PATH = Path(__file__).parent / "config.yaml"


def load_config(config_path: str = None) -> dict:
    """加载配置文件"""
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH

    if not path.exists():
        raise FileNotFoundError(f"配置文件不存在: {path}")

    with open(path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    return config


def get_config() -> dict:
    """获取配置（单例模式）"""
    return load_config()
