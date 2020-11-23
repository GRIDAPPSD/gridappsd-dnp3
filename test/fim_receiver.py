
from gridappsd import GridAPPSD, DifferenceBuilder, utils
from time import sleep

def on_message(simulation_id,message):
    print("Message received:", simulation_id['message-id'])
    print(message)
    # if(dnp3_object_list.__len__() > 0):
    #     updates = dnp3_object_list[0].create_message_updates(simulation_id, message)
    #     if updates is None:
    #         print("NONE Jeff")
    #         return
    #     print("Outstation Updates Created")
    #
    #     for cimMapping in dnp3_object_list:
    #         cimMapping.outstation.apply_compiled_updates(updates)

        # print("Done updating outstations")

if __name__ == '__main__':

    simulation_id = str(1234)
    gapps = GridAPPSD(simulation_id, address=utils.get_gridappsd_address(),
                      username=utils.get_gridappsd_user(), password=utils.get_gridappsd_pass())

    gapps.subscribe('/topic/goss.gridappsd.fim.input.' + simulation_id, on_message)
    while True:
        sleep(0.01)