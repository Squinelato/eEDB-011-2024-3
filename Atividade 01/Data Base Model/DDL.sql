-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema mydb
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema mydb
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `mydb` DEFAULT CHARACTER SET utf8 ;
USE `mydb` ;

-- -----------------------------------------------------
-- Table `mydb`.`rwzd_bank`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `mydb`.`rwzd_bank` (
  `financial_institution` VARCHAR(256) NOT NULL,
  `cnpj` VARCHAR(20) NULL,
  `segment` VARCHAR(4) NULL,
  PRIMARY KEY (`financial_institution`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `mydb`.`rwzd_employee`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `mydb`.`rwzd_employee` (
  `financial_institution` VARCHAR(256) NOT NULL,
  `employer_name` VARCHAR(256) NULL,
  `reviews_count` INT NULL,
  `culture_count` INT NULL,
  `salaries_count` INT NULL,
  `benefits_count` INT NULL,
  `employer_website` VARCHAR(256) NULL,
  `employer_headquarters` VARCHAR(256) NULL,
  `employer_founded` VARCHAR(4) NULL,
  `employer_industry` VARCHAR(256) NULL,
  `employer_revenue` VARCHAR(256) NULL,
  `url` VARCHAR(256) NULL,
  `general_score` FLOAT NULL,
  `culture_values_score` FLOAT NULL,
  `diversity_inclusion_score` FLOAT NULL,
  `life_quality_score` FLOAT NULL,
  `strong_leadership_score` FLOAT NULL,
  `compensation_benefits_score` FLOAT NULL,
  `career_opportunities_score` FLOAT NULL,
  `recommendation_score` FLOAT NULL,
  `company_positive_score` FLOAT NULL,
  `segment` VARCHAR(4) NULL,
  `match_percent` INT NULL,
  PRIMARY KEY (`financial_institution`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `mydb`.`rwzd_claim`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `mydb`.`rwzd_claim` (
  `financial_institution` VARCHAR(256) NOT NULL,
  `year` VARCHAR(4) NOT NULL,
  `quarter` VARCHAR(2) NOT NULL,
  `categoty` VARCHAR(256) NULL,
  `type` VARCHAR(256) NULL,
  `cnpj` INT NULL,
  `index` FLOAT NULL,
  `number_of_regulated_complaints_received` INT NULL,
  `number_of_regulated_complaints_others` INT NULL,
  `number_of_unregulated_complaints` INT NULL,
  `total_number_of_complaints` INT NULL,
  `total_number_of_ccs_and_scr_customers` INT NULL,
  `number_of_ccs_customers` INT NULL,
  `number_of_scr_customers` INT NULL,
  PRIMARY KEY (`financial_institution`, `year`, `quarter`))
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
