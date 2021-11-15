from queue import Queue
from threading import Thread

from .util import chunks


def _do_work(q: Queue, fnc, fix_param: tuple, rt: list, rt_null: list):
    """
    Ejecuta una función sobre los elementos de una Queue

    :param q: Queue que nos da los parámetros sobre el que ejecutar la función
    :param fnc: Función a ejecutar
    :param fix_param: Parámetros fijos de la función
    :param rt: Lista a poblar con los resultados de la función
    :param rt_null: Lista a poblar con los argumentos que hicieron a la función devolver None
    """
    while not q.empty():
        args = q.get()
        r = fnc(*(fix_param + args))
        if r is None:
            rt_null.append(args[0] if len(args) == 1 else args)
        else:
            if isinstance(r, list):
                rt.extend(r)
            else:
                rt.append(r)
        q.task_done()
    return True


class ThreadMe:
    def __init__(self, fix_param=None, max_thread=10, list_size=2000):
        """
        Paraleliza una función sobre elementos de una lista

        :param fix_param: Parámetros fijos que se van a pasar a cada trabajo
        :param max_thread: Máximo numero de hilos a utilizar
        :param list_size: Tamaño máximo de la lista a devolver
        """
        self.max_thread = max_thread
        if fix_param is None:
            fix_param = tuple()
        elif not isinstance(fix_param, tuple):
            fix_param = (fix_param,)
        self.fix_param = fix_param
        self.list_size = list_size
        self.rt_null = []

    def run(self, do_work, data, return_first: bool = False):
        """
        Ejecuta la función para cada elemento de la lista y va devolviendo los resultados con un generador

        :param do_work: función a ejecutar
        :param data: datos sobre los que se ejecuta la función
        :param return_first: indica si el primer elemento ha de devolverse tal cual

        :return: Generador con los resultados de la función
        """
        if return_first:
            for i in next(data):
                yield i
        for dt in chunks(data, self.max_thread):
            q = Queue(maxsize=0)
            rt = []
            for d in dt:
                if not isinstance(d, tuple):
                    d = (d,)
                q.put(d)
            for i in range(len(dt)):
                worker = Thread(target=_do_work, args=(q, do_work, self.fix_param, rt, self.rt_null))
                worker.setDaemon(True)
                worker.start()
            q.join()
            for i in rt:
                yield i

    def list_run(self, *args, **kwargs):
        """
        Ejecuta la función para cada elemento de la lista y va devolviendo los resultados con un generador
        de listas de resultados de tamaño máximo determinado por self.list_size
        """
        for arr in chunks(self.run(*args, **kwargs), self.list_size):
            yield arr
