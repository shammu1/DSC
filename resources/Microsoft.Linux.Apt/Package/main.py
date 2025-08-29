import sys
import json
from AptPackage import AptPackage 
from logger import dfl_logger as Logger


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("no operation specified (expected: get, set, list, test)")
        sys.exit(1)
    operation = sys.argv[1].lower()


    input_data = sys.stdin.read()
    sys.stderr.write(f"Input Data: {input_data}")
    if input_data:
        pkg = AptPackage.from_json(input_data)
        sys.stderr.write(f"Package created: {pkg.name}")

        if operation == "get":
            print(pkg.get())  # must return json string
        elif operation == "set":
            print(pkg.set())
        elif operation == "delete":
            print(pkg.delete())
        elif operation == "test":
            print(json.dumps({"result": pkg.is_installed()}))
        elif operation == "export":
            print(json.dumps(AptPackage.export(pkg)))
    # elif operation == "export":
    #     #if input_data:
    #     #   print(json.dumps(aptpackage.export(pkg)))
    #     #else:
    #     print(json.dumps(AptPackage.export()))
    else:
        print(f"unknown operation: {operation}")
        sys.exit(1)


    # if operation in ["get", "set", "delete", "test"]:
    #     input_data = sys.stdin.read()
    #     pkg = AptPackage.from_json(input_data)

    #     if operation == "get":
    #         print(pkg.get())  # must return json string
    #     elif operation == "set":
    #         print(pkg.set())
    #     elif operation == "delete":
    #         print(pkg.delete())
    #     elif operation == "test":
    #         print(json.dumps({"result": pkg.is_installed()}))
    # elif operation == "export":
    #     #if input_data:
    #     #   print(json.dumps(aptpackage.export(pkg)))
    #     #else:
    #     print(json.dumps(AptPackage.export()))
    # else:
    #     print(f"unknown operation: {operation}")
    #     sys.exit(1)
