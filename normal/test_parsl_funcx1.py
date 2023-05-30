import time
# from funcx import FuncXClient

# fxc = FuncXClient()

# from funcx import FuncXExecutor


# First, define the function ...
def add_func(a, b):
    return a + b


def test_parsl():
    import parsl
    import os
    from parsl.app.app import python_app, bash_app
    from parsl.configs.local_threads import config

    print(parsl.__version__)
    parsl.load(config)

    # App that estimates pi by placing points in a box
    @python_app
    def pi(num_points):
        from random import random

        inside = 0
        for i in range(num_points):
            x, y = random(), random()  # Drop a random point in the box.
            if x**2 + y**2 < 1:        # Count points within the circle.
                inside += 1

        return (inside*4 / num_points)

    # App that computes the mean of three values
    @python_app
    def mean(a, b, c):
        return (a + b + c) / 3

    # Estimate three values for pi
    a, b, c = pi(10**6), pi(10**6), pi(10**6)

    # Compute the mean of the three estimates
    mean_pi  = mean(a, b, c)

    # Print the results
    print("a: {:.5f} b: {:.5f} c: {:.5f}".format(a.result(), b.result(), c.result()))
    print("Average: {:.5f}".format(mean_pi.result()))
    return mean_pi.result()

if __name__ == "__adsdmain__":

    from globus_compute_sdk import Client
    from globus_compute_sdk import Executor

    fxc = Client()

    tutorial_endpoint_id = '4b116d3c-1703-4f8f-9f6f-39921e5864df' # Public tutorial endpoint

    my_endpoint_id = "d614a625-a276-453b-afdc-42422e327573"

    endpoint_id = my_endpoint_id

    func_uuid = fxc.register_function(test_parsl)
    print("hello_world uuid:")
    print(func_uuid)


    res = fxc.run(endpoint_id=endpoint_id, function_id=func_uuid)
    print("run %s" % func_uuid)
    print(res)

    task = fxc.get_task(res)
    while task['pending'] is True:
        time.sleep(0.1)
        task = fxc.get_task(res)
    ret = fxc.get_result(res)
    print(ret)

