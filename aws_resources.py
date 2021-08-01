import configparser


def load_credentials():
    config = configparser.ConfigParser()
    config.read_file((open(r'dwh.cfg')))
    path1 = config.get('CLUSTER', 'HOST')


if __name__ == "__main__":
    load_credentials()