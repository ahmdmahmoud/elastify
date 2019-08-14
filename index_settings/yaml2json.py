import yaml, json, sys

if __name__ == '__main__':
    json.dump(yaml.load(sys.stdin), sys.stdout, indent=2)
