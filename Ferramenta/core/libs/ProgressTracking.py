# -*- encoding: utf-8 -*-
#!/usr/bin/python
from datetime import datetime

class ProgressTracker:
    _next_percentage: int = 0
    estimated_completion_time: str = ''
    percentage_base = 10
    current: int = 0
    total: int = 0
    name: str = ''

    def init_tracking(self, total: int, name: str) -> None:
        self._start_time = datetime.now()
        self.current = 0
        self.total = int(total)
        self.name = name
        self._next_percentage = 0
        self.report_progress(0)
    
    def report_progress(self, current: int = 0, add_progress: bool = False) -> None:
        if add_progress:
            self.current += 1
        elif current:
            self.current = current
        
        if not self.current or self.current_percentage > self._next_percentage:
            if self.current:
                self.estimate_completion_time()
            self._next_percentage += 1
            # print(f"{self.name} |{self.progress}{self.remaining_progress}| {self.estimated_completion_time}", end="\r")
            print(f"{self.name} |{self.progress}{self.remaining_progress}| {self.estimated_completion_time}", end="\n")
    
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

    def estimate_completion_time(self) -> None:
        elapsed_time = datetime.now() - self._start_time
        average_time = elapsed_time.seconds / self.current
        estimated_time = average_time * (self.total - self.current)
        self.estimated_completion_time = f'{estimated_time:.2f} seconds'
