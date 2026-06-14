
CREATE TABLE IF NOT EXISTS pets (
    id SERIAL PRIMARY KEY,               
    name VARCHAR(100) NOT NULL,          
    species VARCHAR(50) NOT NULL,         
    fav_foods TEXT,                       
    birth_year INTEGER,                   
    photo_url TEXT,                   
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  
);

-- Комментарии для документирования
COMMENT ON TABLE pets IS 'Таблица с данными о питомцах из JSON';
COMMENT ON COLUMN pets.name IS 'Имя питомца';
COMMENT ON COLUMN pets.species IS 'Вид животного (Cat/Dog/etc)';

-- Создаем индекс для быстрого поиска по виду
CREATE INDEX IF NOT EXISTS idx_pets_species ON pets(species);