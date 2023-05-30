#!/usr/bin/env python


def test_function(funct_str):
    exec(funct_str)
    print(locals())
    name = 'test_print'
    f = locals()[name]
    return f


if __name__ == "__main__":
    funct_str = "print('test')"
    funct_str = """
def test_print():
    print("test_print")
"""
    f= test_function(funct_str)
    f()
