<?php

namespace Pagewiser\Database\Migration;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Output\OutputInterface;


/**
 * Import database alter scripts from directory
 *
 * Iterate files in provided directory and execute the ones that are not imported.
 */
class DatabaseUpgradeCommand extends Command
{

	/**
	 * @var string $databaseName Database name
	 */
	private $databaseName;

	/**
	 * @var string $tableName Name of table to store imported files
	 */
	private $tableName = '_sql_migrate';

	/**
	 * @var \DibiConnection $dibi Dibi
	 */
	private $dibi;

	/**
	 * @var string $dbDir SQL files directory path
	 */
	private $dbDir;

	/**
	 * @var bool $dryRun Perform dry run
	 */
	private $dryRun = FALSE;

	/**
	 * @var bool $mark Mark files as imported without executing any query
	 */
	private $mark = FALSE;

	/**
	 * @var bool $ignoreErrors Ignore SQL errors, try to load as much as possible
	 */
	private $ignoreErrors = FALSE;

	/**
	 * @var bool $smokeTest Run smoke test
	 */
	private $smokeTest = FALSE;


	/**
	 * Configure command line arguments
	 */
	protected function configure()
	{
		$this->setName('database:upgrade')
			->addArgument('dir', InputArgument::REQUIRED, 'Directory with SQL scripts')
			->addOption('dry-run', NULL, InputOption::VALUE_NONE, 'Dry run will only output what will be done, doesn\'t do anything.')
			->addOption('ignore', NULL, InputOption::VALUE_NONE, 'Ignore errors in SQL files, try to import everything.')
			->addOption('mark', NULL, InputOption::VALUE_NONE, 'Mark all scripts as imported without actualy importing them.')
			->addOption('smoke-test', NULL, InputOption::VALUE_NONE, 'Run smoke test, create database, run all script and drop database.')
			->addOption('database', NULL, InputOption::VALUE_OPTIONAL, 'Change database name.')
			->setDescription('Upgrade database with data from db folder');
	}


	/**
	 * Initialize command line command and validate input parameters
	 *
	 * @param \Symfony\Component\Console\Input\InputInterface $input
	 * @param \Symfony\Component\Console\Output\OutputInterface $output
	 */
	protected function initialize(InputInterface $input, OutputInterface $output)
	{
		$dir = $input->getArgument('dir');

		if (!file_exists($dir) || !is_dir($dir))
		{
			throw new \RuntimeException('Directory '.$dir.' does not exists.');
		}

		$this->dbDir = $dir;
		$this->databaseName = $input->getOption('database');

		$this->dryRun = (bool) $input->getOption('dry-run');
		$this->mark = (bool) $input->getOption('mark');
		$this->ignoreErrors = (bool) $input->getOption('ignore');
		$this->smokeTest = (bool) $input->getOption('smoke-test');
	}


	/**
	 * Import all files in database directory
	 *
	 * @param \Symfony\Component\Console\Input\InputInterface $input
	 * @param \Symfony\Component\Console\Output\OutputInterface $output
	 *
	 * @throws \DibiDriverException When file contains errors
	 * @throws \Exception Unexpected error
	 *
	 * @return NULL
	 */
	protected function execute(InputInterface $input, OutputInterface $output)
	{
		/** @var \DibiConnection $dibi */
		$dibi = $this->dibi = clone $this->getHelper('container')->getByType('DibiConnection');

		$this->prepareSmokeTest();

		if ($this->databaseName)
		{
			$dibi->query('use %n', $this->databaseName);
		}

		$imported = $this->getImportedFiles($output);

		$runArray = [];
		$d = dir($this->dbDir);
		while (false !== ($entry = $d->read()))
		{
			if (preg_match('/^([0-9]+).*\.sql$/', $entry) && !in_array($entry, $imported))
			{
				$runArray[] = $entry;
			}
		}
		$d->close();

		if (empty($runArray))
		{
			$this->finishSmokeTest();
			return;
		}

		natsort($runArray);

		// zero for succesfull run or number of errors
		$errorCode = 0;

		foreach ($runArray as $file)
		{
			$errors = 0;

			if ($this->dryRun)
			{
				$output->writeln('<options=bold>Run will import file: '.$file.'</>');
				continue;
			}

			if ($this->mark)
			{
				$output->writeln('<options=bold>Marking file as imported: '.$file.'</>');
				$dibi->insert($this->tableName, array('file' => $file, 'errors' => '0'))->execute();
				continue;
			}

			$output->writeln('<options=bold>Importing file: '.$file.'</>');

			try
			{
				$cnt = $dibi->loadFile($this->dbDir . '/' . $file);
				$output->writeln('  - succesfull queries count: ' . $cnt);
			}
			catch (\DibiDriverException $ex)
			{
				$output->writeln('<fg=white;bg=red>ERROR IN ' . $file . ' FILE</>');
				if (!$this->ignoreErrors)
				{
					$dibi->insert($this->tableName, array('file' => $file, 'errors' => 1))->execute();
					$this->finishSmokeTest();
					throw $ex;
				}
				$errors = 1;
			}

			$dibi->insert($this->tableName, array('file' => $file, 'errors' => $errors))->execute();
		}

		$this->setCode(function() use ($errorCode) { return $errorCode; });

		$this->finishSmokeTest();
	}


	/**
	 * Get already imported files
	 *
	 * @param \Symfony\Component\Console\Output\OutputInterface $output
	 *
	 * @return array
	 */
	private function getImportedFiles(OutputInterface $output)
	{
		$dibi = $this->dibi;

		$res = $dibi->query('SHOW TABLES LIKE %s', $this->tableName)->fetchAll();
		if (!$res)
		{
			$dibi->query(
				"CREATE TABLE %n (
					id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'Primarny key',
					file VARCHAR( 255 ) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL COMMENT 'Filename',
					errors INT UNSIGNED NOT NULL DEFAULT '0' COMMENT 'Errors',
					created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'timestamp'
				) ENGINE = INNODB CHARACTER SET ascii COLLATE ascii_general_ci COMMENT 'SQL migration script'",
				$this->tableName
			);
		}

		$output->isVerbose() && $output->writeln('<options=bold>Already imported files:</>', OutputInterface::VERBOSITY_VERBOSE);
		$imported = [];
		$query = $dibi->query("SELECT file FROM %n", $this->tableName);
		foreach ($query as $row)
		{
			$output->isVerbose() && $output->writeln('  ' . $row['file'], OutputInterface::VERBOSITY_VERBOSE);
			$imported[] = $row['file'];
		}

		return $imported;
	}


	/**
	 * Run before smoke test
	 *
	 * Create test database.
	 *
	 * @return NULL
	 */
	private function prepareSmokeTest()
	{
		if (!$this->smokeTest)
		{
			return;
		}

		if (!$this->databaseName)
		{
			throw new \RuntimeException('database parameter is required when running smoke test');
		}

		$res = $this->dibi->query('SHOW DATABASES LIKE %s', $this->databaseName)->fetch();
		if ($res)
		{
			throw new \RuntimeException('Database already exists, cannot perform smoke test.');
		}

		$this->dibi->query('CREATE DATABASE %n COLLATE %s;', $this->databaseName, 'utf8_general_ci');
	}


	/**
	 * Run after smoke test
	 *
	 * Drop test database.
	 *
	 * @return NULL
	 */
	private function finishSmokeTest()
	{
		if (!$this->smokeTest)
		{
			return;
		}

		$this->dibi->query('DROP DATABASE IF EXISTS %n;', $this->databaseName);
	}


}
