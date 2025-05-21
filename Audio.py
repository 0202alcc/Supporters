import librosa
import numpy as np
import matplotlib.pyplot as plt
import IPython.display as ipd
import pandas as pd
from pathlib import Path

class Audio:
    """
    A class to handle audio file processing and analysis.
    
    This class provides functionality to load audio files, convert them to DataFrames,
    and perform basic audio analysis operations.
    
    Attributes:
        audio_path (str): Path to the audio file
        audio (numpy.ndarray): Raw audio data
        sr (int): Sample rate of the audio
        original_df (pandas.DataFrame): DataFrame containing raw amplitude data
        seconds_df (pandas.DataFrame): DataFrame containing amplitude and time data
    """
    
    def __init__(self, audio_path, listen=False):
        """
        Initialize the Audio class with an audio file.
        
        Args:
            audio_path (str): Path to the audio file
            listen (bool, optional): Whether to play the audio on initialization. Defaults to False.
            
        Raises:
            FileNotFoundError: If the audio file doesn't exist
            ValueError: If the audio file can't be loaded
        """
        try:
            # Validate file exists
            if not Path(audio_path).is_file():
                raise FileNotFoundError(f"Audio file not found: {audio_path}")
            
            self.audio_path = audio_path
            self.audio, self.sr = librosa.load(self.audio_path, sr=None)  # sr=None preserves the original sample rate
            
            # Create DataFrames
            self.original_df = pd.DataFrame({'amplitude': self.audio})
            self.time = np.arange(len(self.original_df)) / self.sr
            self.seconds_df = pd.DataFrame({
                'amplitude': self.audio,
                'time': self.time
            })
            
            if listen:
                ipd.Audio(self.audio, rate=self.sr)
                
        except Exception as e:
            raise ValueError(f"Error loading audio file: {str(e)}")

    def convert_to_dataframe(self):
        """
        Convert the audio data to pandas DataFrames.
        
        Returns:
            tuple: A tuple containing:
                - dict: Dictionary with original DataFrame and sample rate
                - pandas.DataFrame: DataFrame with amplitude and time data
        """
        return {
            'original_df': self.original_df,
            'sample_rate': self.sr,
        }, self.seconds_df