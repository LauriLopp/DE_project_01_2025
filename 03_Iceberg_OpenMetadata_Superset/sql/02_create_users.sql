-- Create users for analysts
CREATE USER IF NOT EXISTS user_full
    IDENTIFIED BY 'strong_password_full';
CREATE USER IF NOT EXISTS user_limited 
    IDENTIFIED BY 'strong_password_limited';