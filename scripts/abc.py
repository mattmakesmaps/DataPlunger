__author__ = 'mkenny'
import abc

class Person(object):
    """
    Creates an abstract base class with two required methods for the interface
    -- __init__()
    -- speak()
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, name, age):
        self.name = name
        self.age = age

    @abc.abstractmethod
    def speak(self, words):
        print words

class Matt(Person):
    """
    A direct subclass of the ABC, Person.
    Note that the only requirement from the ABC parent at this point is
    to implement the required interface methods.

    The calling signitures of the methods themselves can be overwritten.
    Additionally, more public and protected methods can be added.
    """
    def __init__(self, name, age, speed):
        self.name = name
        self.age = age
        self.speed = speed

    def _speak(self, words):
        return words.upper()

    def speak(self, words):
        return self._speak(words)

    def run(self):
        print "running"


class Scott(Matt):
    """
    Since this class is not a direct sublcass of the ABC, but rather a subclass
    of a sublcass, we aren't bound to implment the ABC's interface in this class.
    """
    pass

if __name__ == '__main__':
    # p = Person('person', 26)
    m = Matt('matt', 27, "fast")
    s = Scott('scott', 30, "fast")
    m_words = m.speak("hello")
    print "Matt Says: %s" % m_words
    print isinstance(m, Person)
    print isinstance(s, Person)
    print issubclass(Matt, Person)
    print issubclass(Scott, Person)

    for subclass in Person.__subclasses__():
        print subclass.__name__
