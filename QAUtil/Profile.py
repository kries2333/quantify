from line_profiler import LineProfiler
import atexit

profile = LineProfiler()
atexit.register(profile.print_stats)