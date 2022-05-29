"""
Clásico problema productor - consumidor.

Desarrollo de una cola consumidora que al consumir ejecute una función cuya entrada será el objeto consumido. 
Por tanto se ejecutará tantas veces como objetos se publiquen en la misma. Extra deseable: permitir seleccionar 
el número de procesos que consumen de la cola para aumentar el paralelismo

"""

import multiprocessing
from multiprocessing import Process, Queue
import logging


# Función para log
def display_log(message):
    """Función dedicada a logging para un mayor seguimiento del proceso.

    Args:
        message: mensaje de log.

    """
    logging.basicConfig(format='%(levelname)s - %(asctime)s.%(msecs)03d: %(message)s', datefmt='%H:%M:%S',
                        level=logging.DEBUG)
    processname = multiprocessing.current_process().name
    logging.info(f'{processname}: {message}')


# Función productor
def produce_queue(queue_to_produce: Queue,
                  max_prod_items: int,
                  finish: list):
    """Función productor. Produce la cola de items a ser consumidos.

    Args:
        queue_to_produce: queue/cola que contendrá los items a consumir.
        max_prod_items: número máximo de items a producir.
        finish: cola cuyo contenido servirá para informar acerca del estado de completitud del productor.

    """
    finish.put(False)
    
    for number in range(max_prod_items):
        display_log(f'produce item: {number}')
        queue_to_produce.put(number)

    # Tras terminar el proceso de producción de la cola, informamos a través de la cola finish que ha terminado.
    finish.put(True)

    display_log('finished')


def get_x2(item: int) -> int:
    """ Función que realizará operaciones sobre los items consumidos.

    Args:
        item: item consumido.

    Returns:
        número entero resultado del producto entre el item consumido y el número 2.

    """
    operation = item * 2
    
    return operation


# Función consumidora.
def consume_queue(queue_to_consume: Queue,
                  finish: list):
    """ Función consumidora. Consume los items de una cola.

    Args:
        queue_to_consume: Cola con los items a consumir.
        finish: lista que informa acerca del estado de progreso del paso de producción.

    """
    while True:

        if not queue_to_consume.empty():
            queue_value = queue_to_consume.get()
            display_log(f'Consuming: {queue_value}')
            x2_value = get_x2(queue_value)
            display_log(f'finished operation: {x2_value}')

        else:
            finish_value = finish.get()
            if finish_value == True:
                break

        display_log('finished')


def main():

    max_prod_items = 250  # número de items que tendrá la cola a consumir.

    # Como se indicó preferible en el enunciado de la prueba. Establecemos una variable que nos permitirá controlar
    # el número de procesos que consumen de la cola:
    consumer_processes_num = 4

    finish = Queue()
    queue_to_produce = Queue()

    producer = Process(target=produce_queue, args=(queue_to_produce, max_prod_items, finish,))
    producer.start()

    # Es en esta parte del código donde, en función de la variable definida anteriormente, desplegamos el número
    # deseado de procesos que consumen de la cola:
    consumer_processes = []
    for num in range(consumer_processes_num):
        consumer = Process(target=consume_queue, args=(queue_to_produce, finish,))
        consumer.start()
        consumer_processes.append(consumer)

    producer.join()
    for process in consumer_processes:
        process.join()


if __name__ == "__main__":
    main()
