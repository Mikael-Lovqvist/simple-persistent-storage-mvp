from efforting.mvp4 import type_system as TS
from efforting.mvp4.type_system.bases import public_base
import time

class base(public_base):
	time = TS.positional()

class access(base):
	path = TS.positional()

class create(access):
	value = TS.positional()

class update(create):
	previous_value = TS.positional()

