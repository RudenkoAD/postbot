from aiogram.fsm.state import State, StatesGroup

class AddGroupStates(StatesGroup):
    link = State()
    accept = State()
    
class ScreenStates(StatesGroup):
    main = State()