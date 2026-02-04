import inspect
import os
import re

from tabsdata.api.tabsdata_server import TabsdataServer

from td_sync.trigger import main as trigger_function


def main(collection_name: str = None, trigger_function_flag: bool = False):
    call_stack = inspect.stack()
    if "tabsdata/api/tabsdata_server.py" not in str(call_stack):
        # Establish Connection to Server
        server = TabsdataServer("127.0.0.1:2457", "admin", "tabsdata", "sys_admin")
        fr = call_stack[1]
        caller_mod = inspect.getmodule(fr.frame)

        # Pull Existing Collections From Server
        collections_list = server.list_collections()
        collection_list_names = [i.name for i in collections_list]

        # Create enumerated dict of collections and string for user input of collection name or index
        options_dict = {i + 1: name for i, name in enumerate(collection_list_names)}
        options_string = "\n".join(f"[{i}] {name}" for i, name in options_dict.items())

        # Define Pattern for valid collection name
        pattern = re.compile(r"^(?=.*[A-Za-z])[A-Za-z0-9_]{1,100}$")

        # Define Flag for Validating Collection
        collection_validated_flag = False

        while collection_validated_flag == False:
            # Handles if no collection provided as function parameter
            if collection_name is None:
                collection_name = input(
                    "Please define a collection to use from the following list, "
                    "or define a new collection to create and use:\n\n"
                    + "\n"
                    + options_string
                    + "\n\n> "
                )

                if pattern.match(collection_name) is None:
                    if collection_name.isdigit():
                        if int(collection_name) in options_dict:
                            collection_name = options_dict[int(collection_name)]
                            collection_validated_flag = True
                        else:
                            collection_name = input(
                                "Index not found. Please enter a valid collection name or valid index: "
                            )
            # Handles if collection name provided is not valid
            elif pattern.match(collection_name) is None:
                collection_name = None
                print(
                    "Collection Name provided is not valid. Please enter a valid collection name or valid existing collection name or index:"
                )
            # Creates Collection if name is valid but not existing
            elif collection_name not in collection_list_names:
                server.create_collection(collection_name)
                collection_validated_flag = True
            # Breaks While Loop
            else:
                collection_validated_flag = True

        # Finds all tabsdata functions in current python file
        functions_in_file = [
            name
            for name, obj in inspect.getmembers(caller_mod, callable)
            if getattr(obj, "__module__", None) in ["tabsdata._tdfunction"]
        ]
        # print(functions_in_file)

        # extract file name and file path for registration/update
        full_path = os.path.abspath(fr.filename)
        file_path = os.path.basename(fr.filename)
        file_name = os.path.splitext(os.path.basename(fr.filename))[0]

        # Defines Function Name, prioritizing function that matches filename, queries function name from user if more than one function present otherwise
        if file_name in functions_in_file:
            function_name = file_name
        elif len(functions_in_file) == 1:
            function_name = functions_in_file[0]
        else:
            function_name = input(
                "No functions with name matching the file name found. Please enter the function name to proceed: "
            )

        # Defines --path flag for function
        function_path = f"{full_path}::{function_name}"

        list_functions = [i.name for i in server.list_functions(collection_name)]

        check_function_exists = function_name in list_functions

        # Updates or Registers the Function
        if check_function_exists:
            server.update_function(
                collection_name,
                function_name,
                function_path,
                description="updated through auto-updater",
            )
            print(
                f"Updated Function with the following parameters\n\nCollection: {collection_name}\nFunction Name: {function_name}"
            )
        else:
            server.register_function(
                collection_name=collection_name, function_path=function_path
            )
            print(
                f"Registered Function with the following parameters\n\nCollection: {collection_name}\nFunction Name: {function_name}"
            )

        if trigger_function_flag == True:
            trigger_function(
                collection_name=collection_name, function_name=function_name
            )
        print()
