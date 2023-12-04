
#### #### #### #### #### #### #### #### #### #### #### #### 
#### #### #### #### #### #### #### #### #### #### #### #### 
#### #### #### #### #### #### #### #### #### #### #### #### 
#### aws.bi.LakeH.hq.prd #<ACCOUNT-ID> #### #### ####
#### #### #### #### #### #### #### #### #### #### #### #### 
#### #### #### #### #### #### #### #### #### #### #### #### 
#### #### #### #### #### #### #### #### #### #### #### #### 

locals {
  profile = "<ACCOUNT-ID>_AWSAdministratorAccess"
  region = "us-east-1"
  file_cp_to_bucket = "glu_hq_finan_hq_zeus_exchange_rates_prd_001"
  bucket_install_script = "s3-hq-raw-prd-intec"
  prefix_install_script = "app_glu_hq_finan_hq_zeus_exchange_rates_prd_001"
  bucket_to_crawl = "s3-hq-std-prd-finan"
  prefix_to_crawl = "xchng_rts"
  svc_role_arn = "arn:aws:iam::<ACCOUNT-ID>:role/svc-role-data-mic-development-integrations"
  crawler_name = "crwl-hq-raw-prd-finan-zeus-exchange-rates"
  db_target = "hq-std-prd-finan-link"
  step_function_name = "stp-fnc-hq-finan-zeus-exchange-prd"
  step_function_path = "zeus_exchange_rates_step_function_prd.json"
}

provider "aws" {
  alias = "aws-bi-LakeH-hq-prd"
  profile = local.profile
  region  = local.region
}

## INSTALAR SCRIPT

# borrar el viejo y copiar el archivo al bucket
resource "null_resource" "copy_source_code" {
  triggers = {
    always_run = "${timestamp()}"
  }
  provisioner "local-exec" {
    command = "aws s3 cp ${local.file_cp_to_bucket}.py s3://${local.bucket_install_script}/${local.prefix_install_script}/ --profile ${local.profile}"
  }
}

## Definición del trabajo de Glue
resource "aws_glue_job" "create_job_exchange_rates_glue" {
  provider = aws.aws-bi-LakeH-hq-prd
  name     = local.file_cp_to_bucket
  role_arn = local.svc_role_arn
  glue_version = "4.0"

  command {
    script_location = "s3://${local.bucket_install_script}/${local.prefix_install_script}/${local.file_cp_to_bucket}.py" 
    python_version  = "3"                          
  }

  default_arguments = {
    "--TempDir"             = "s3://${local.bucket_install_script}/${local.prefix_install_script}/temp-dir/"
    "--job-bookmark-option" = "job-bookmark-enable"
    #"--extra-py-files"      = "s3://${local.bucket_to_create}/extra-files.zip"  # Si necesitas archivos adicionales
  }

  max_retries  = 0      # Número de reintentos en caso de fallo
  timeout      = 60     # Tiempo máximo de ejecución en minutos
  number_of_workers = 5 # numero de workers del job
  worker_type = "G.1X"  # tipo de worker
}


## CONFIGURAR CRAWLERS
# la step function se encuentra en otra cuenta


resource "aws_glue_crawler" "exchange_rates_crawler" {
  provider = aws.aws-bi-LakeH-hq-prd
  name          = local.crawler_name
  role          = local.svc_role_arn  # Asegúrate de reemplazar esto con el ARN de tu rol de IAM para Glue

  database_name = "${local.db_target}"  # Reemplaza con el nombre de tu base de datos de Glue

  s3_target {
    path = "s3://${local.bucket_to_crawl}/${local.prefix_to_crawl}/"
  }


}

# ## CONFIGURAR STEP FUNCTION

resource "aws_sfn_state_machine" "daily_rate_state_machine" {
  provider = aws.aws-bi-LakeH-hq-prd
  name     = local.step_function_name
  role_arn = local.svc_role_arn

  # Lee la definición de la máquina de estados del archivo JSON
  definition = file("${local.step_function_path}")
}




