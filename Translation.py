from deep_translator import GoogleTranslator

print("Enter the english keyword:")
engl_Keyword = input()

def Translation():
    chin_Keyword = GoogleTranslator(source='en', target='zh-cn').translate(engl_Keyword)
    print(chin_Keyword)

Translation()




