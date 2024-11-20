from multiprocessing import Process
import os
from datetime import datetime

# Definition of Indices functions
def AllCapIndex():
    import Review_Process_Function_2012_Relaxed_DEV_Country_All_Cap_Optimized

def StandardIndex():
    import Review_Process_Function_2012_Relaxed_DEV_Country_Standard_Optmized

def Carve_Out_Small():
    import Carve_Out_SC

def iStudio_Creator():
    import iStudio_Creator

def Remove_Shadow():
    import Remove_ALLCAP_Shadow

# Main entry point
if __name__ == '__main__':

    # Define Global Variables
    os.environ["CN_Target_Percentage"] = str(0.90)
    os.environ["GMSR_Upper_Buffer"] = str(0.99) # 0.99
    os.environ["GMSR_Lower_Buffer"] = str(0.9925) # 0.9925
    os.environ["current_datetime"] = datetime.today().strftime('%Y%m%d')

    # Create and start Process_1
    Process_1 = Process(target=AllCapIndex)
    Process_1.start()
    Process_1.join()
    
    # Create and start Process_2 after Process_1 is finished
    Process_2 = Process(target=StandardIndex)
    Process_2.start()
    Process_2.join()

    # Create and start Process_3 after Process_2 is finished
    Process_3 = Process(target=Remove_Shadow)
    Process_3.start()
    Process_3.join()

    # Create and Start Process_4 after Process_4 is finished
    Process_4 = Process(target=Carve_Out_Small)
    Process_4.start()
    Process_4.join()

    # Create and Start Process_5 after Process_4 is finished
    Process_5 = Process(target=iStudio_Creator)
    Process_5.start()
    Process_5.join()