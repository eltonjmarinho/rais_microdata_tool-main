import tkinter as tk
import multiprocessing
import threading
from queue import Empty

from src.ui.main_window import MainApplicationWindow
from src.ui.splash_screen import SplashScreen
from src.controllers.ftp_service import FTPService

def check_queue(root, queue, splash):
    try:
        message = queue.get_nowait()
        msg_type, value = message

        if msg_type == "LOG":
            splash.update_status(value)
        elif msg_type == "FETCH_COMPLETE":
            splash.close()
            app = MainApplicationWindow(root, queue, fetched_data=value)
            return # Stop checking the queue
    except Empty:
        pass
    
    root.after(100, check_queue, root, queue, splash)

if __name__ == "__main__":
    multiprocessing.freeze_support()
    
    mp_queue = multiprocessing.Queue()
    
    root = tk.Tk()
    app_icon = tk.PhotoImage(file='img/Logoapp.png')
    root.iconphoto(False, app_icon)
    root.withdraw() # Hide the root window initially

    splash = SplashScreen(root)
    
    ftp_service = FTPService(mp_queue)
    
    fetch_thread = threading.Thread(target=ftp_service.fetch_available_data, daemon=True)
    fetch_thread.start()
    
    check_queue(root, mp_queue, splash)
    
    root.mainloop()
