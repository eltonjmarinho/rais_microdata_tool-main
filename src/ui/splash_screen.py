import tkinter as tk
from tkinter import ttk
from PIL import Image, ImageTk

class SplashScreen(tk.Toplevel):
    def __init__(self, root):
        super().__init__(root)
        self.root = root
        self.title("Carregando...")
        self.geometry("400x250")
        self.overrideredirect(True) # Remove window decorations

        # Center the splash screen
        self.center_window()

        # Logo
        try:
            self.logo_image = Image.open("img/Raislogo.png")
            # Resize the image to fit the splash screen
            self.logo_image = self.logo_image.resize((300, 150), Image.Resampling.LANCZOS)
            self.logo_photo = ImageTk.PhotoImage(self.logo_image)
            logo_label = tk.Label(self, image=self.logo_photo)
            logo_label.pack(pady=10)
        except FileNotFoundError:
            logo_label = tk.Label(self, text="Logo não encontrada")
            logo_label.pack(pady=10)

        # Status label
        self.status_label = ttk.Label(self, text="Iniciando...")
        self.status_label.pack(pady=5)

        # Progress bar
        self.progress = ttk.Progressbar(self, orient="horizontal", length=350, mode="indeterminate")
        self.progress.pack(pady=10)
        self.progress.start(10)

        self.root.withdraw() # Hide the main window until loading is complete

    def center_window(self):
        self.update_idletasks()
        width = self.winfo_width()
        height = self.winfo_height()
        if width == 1 and height == 1:
            width = 400
            height = 250
        screen_width = self.winfo_screenwidth()
        screen_height = self.winfo_screenheight()
        center_x = int(screen_width/2 - width / 2)
        center_y = int(screen_height/2 - height / 2)
        self.geometry(f'{width}x{height}+{center_x}+{center_y}')

    def update_status(self, message):
        self.status_label.config(text=message)

    def close(self):
        self.progress.stop()
        self.destroy()
        self.root.deiconify() # Show the main window
