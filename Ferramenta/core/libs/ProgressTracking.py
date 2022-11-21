# -*- encoding: utf-8 -*-
#!/usr/bin/python
from datetime import datetime
from core._logs import *

class ProgressTracker:
    _next_percentage: int = 0
    estimated_completion_time: str = ''
    percentage_base = 10
    current: int = 0
    total: int = 0
    name: str = ''

    @property
    def _reset_tracker(self):
        self.current = 0
        self._start_time = datetime.now()
        self._next_percentage = 0

    def init_tracking(self, total: int, name: str) -> None:
        self._reset_tracker
        self.total = int(total)
        self.name = name
    
    def report_progress(self, current: int = 0, add_progress: bool = False) -> None:
        if add_progress:
            self.current += 1
        elif current:
            self.current = current
        
        if not self.current or self.current_percentage > self._next_percentage:
            if self.current:
                self.estimate_completion_time()
            self._next_percentage += 1
            if self.estimated_completion_time < 0:
                return
            aprint(f" - {self.name}:\n    |{self.progress}{self.remaining_progress}| Estimativa de conclusão: {self.estimated_completion_time_str}")
    
    @property
    def remaining_progress(self) -> str:
        return '.'*(self.percentage_base-self.current_percentage)

    @property
    def progress(self) -> str:
        return '|'*self.current_percentage
    
    @property
    def current_percentage(self) -> str:
        if not self.total:
            return 0
        return int((self.current/self.total)*self.percentage_base)

    @property
    def estimated_completion_time_str(self):
        return f'{self.estimated_completion_time/60:.2f} minutos'

    def estimate_completion_time(self) -> None:
        elapsed_time = datetime.now() - self._start_time
        average_time = elapsed_time.seconds / self.current
        self.estimated_completion_time = average_time * (self.total - self.current)
        
