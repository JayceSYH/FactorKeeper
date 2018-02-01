class ServiceDebugger(object):
    __enable_debug = True

    @classmethod
    def set_debug(cls, enable=True):
        cls.__enable_debug = enable

    @classmethod
    def debug(cls, show_form=True, show_param=True, show_response=True, count_time=True, content_limit=100, disable=False):
        from flask import request
        from functools import wraps
        from datetime import datetime
        import traceback

        if not cls.__enable_debug:
            def empty_wrapper(func):
                return func
            return empty_wrapper

        def make_wrapper(func):
            @wraps(func)
            def wrapper(**kwargs):
                if not disable:
                    print("-" * 10)
                    print("Service:{}".format(func.__name__))
                    if show_param:
                        print("Param:")
                        for name in kwargs:
                            val = kwargs[name]
                            print("\t{0}: {1}".format(name, val))

                    if show_form:
                        print("FormData:")
                        for name in request.form:
                            print("\t{0}: {1}".format(name, str(request.form.get(name))[:content_limit]))

                    if count_time:
                        start_time = datetime.now()
                        print("StartTime:{}".format(start_time))

                try:
                    resp = func(**kwargs)
                except:
                    print(traceback.format_exc())
                    resp = ""
                    traceback.print_exc()

                if not disable:
                    if count_time:
                        end_time = datetime.now()
                        print("EndTime:{}".format(end_time))
                        print("TimeCost:{}".format(end_time - start_time))

                    if show_response:
                        print("Return:" + resp[:content_limit])

                return resp

            return wrapper
        return make_wrapper