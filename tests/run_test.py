import os
import sys
import pathlib


if __name__ == '__main__':
    Emergent_path = pathlib.Path(__file__).absolute().parent.parent
    test_path = os.path.join(Emergent_path, 'tests')
    EmergentMain_path = os.path.join(Emergent_path, 'Emergent')
    sys.path.append(test_path)
    sys.path.append(EmergentMain_path)
    test_filename = sys.argv[1]
    test = __import__(test_filename)
    test.run()
