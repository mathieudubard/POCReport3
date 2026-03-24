def remove_empty_values(data: dict):
    return {k: v for k, v in data.items() if v}
