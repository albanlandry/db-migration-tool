-- Dummy member Table
CREATE TABLE member (
    id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    phone VARCHAR(20),
    address VARCHAR(200),
    registration_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

DELIMITER $$

CREATE PROCEDURE populate_member(IN numRows INT)
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= numRows DO
        INSERT INTO member (first_name, last_name, email, phone, address)
        VALUES (
            CONCAT('First', i),
            CONCAT('Last', i),
            CONCAT('user', i, '@example.com'),
            CONCAT('555-', LPAD(FLOOR(RAND()*10000), 4, '0')),
            CONCAT(i, ' Random St')
        );
        SET i = i + 1;
    END WHILE;
END$$

DELIMITER ;

CALL populate_member(50);

-- Dummy post table
CREATE TABLE posts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    member_id INT,  -- Assuming you want to relate this to a member
    title VARCHAR(255) NOT NULL,
    content TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (member_id) REFERENCES member(id)
);

DELIMITER $$

CREATE PROCEDURE populate_posts(IN numRows INT)
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= numRows DO
        INSERT INTO posts (member_id, title, content)
        VALUES (
            FLOOR(1 + RAND() * 50),  -- Random member_id between 1 and 50
            CONCAT('Post Title ', i),
            CONCAT('This is the content for post ', i, '. Lorem ipsum dolor sit amet, consectetur adipiscing elit.')
        );
        SET i = i + 1;
    END WHILE;
END$$

DELIMITER ;

CALL populate_posts(50);
