#' Lecture et division des fichiers BMO
#'
#' @param chemin_data Le chemin du fichier
#' @param siret Nom de la variable SIRET
#' @param reg Nom de la variable région
#' @param dept Nom de la variable département
#' @param cdcmet Nom de la variable commune
#' @param be  Nom de la variable bassin
#' @param taille Nom de la variable taille
#' @param naf Nom de la variable NAF 732
#' @param ponder Nom de la variable de pondération
#' @param annee L'année de l'enquête
#'
#' @return Rien. Deux fichiers sauvegardés
#' 
#' @export
#' 
#' @importFrom magrittr %>%
#' @importFrom haven read_sas
#' @importFrom stringr str_sub
#' @import dplyr
#' @import tidyr
#'
read_split_bmo <- function(chemin_data,
                           siret,
                           reg,
                           dept,
                           cdcmet,
                           be,
                           taille,
                           naf,
                           ponder,
                           annee) {
  
  # chargement des données, sélection et renommage
  df_bmo_init <- read_sas(
    chemin_data,
    col_select = c(
      {{ siret }},
      {{ reg }},
      {{ dept }},
      {{ cdcmet }},
      {{ be }},
      {{ taille }},
      {{ naf }},
      {{ ponder }},
      source,
      A0Z40:SW1Z80
    )
  ) %>% 
    rename(siret = {{ siret }},
           reg = {{ reg }},
           dep = {{ dept }},
           cdcmet = {{ cdcmet }},
           bassin_bmo_annee = {{ be }},
           taille = {{ taille }},
           naf732 = {{ naf }},
           ponder = {{ ponder }}) %>% 
    mutate(annee = {{ annee }})
  
  # export des infos
  df_bmo_init %>% 
    select(siret, annee, reg, dep, bassin_bmo_annee, cdcmet, naf732, taille, source, ponder) %>% 
    mutate_at(vars(siret, reg, dep, cdcmet, bassin_bmo_annee, taille, naf732), as.character) %>%  # remplacer par mutate(across()) avec dplyr 1.0.0
    write_rds(glue("data-raw/inter/df_bmo_init_infos_{annee}.rds"))

  # transposition projets totaux
  df_bmo_init_proj_tot <- df_bmo_init %>%
    select(siret, annee, A0Z40:W1Z80) %>%
    pivot_longer(c(A0Z40:W1Z80), names_to = "fap225", values_to = "nb_proj_tot") %>%
    filter(nb_proj_tot > 0)

  # transposition indicatrice difficulté
  df_bmo_init_proj_diff <- df_bmo_init %>%
    select(siret, XA0Z40:XW1Z80) %>%
    pivot_longer(c(XA0Z40:XW1Z80), names_to = "fap225", values_to = "ind_proj_diff") %>%
    mutate(fap225 = str_sub(fap225, 2, 6))

  # transposition projets saisonniers
  df_bmo_init_proj_sais <- df_bmo_init %>%
    select(siret, SA0Z40:SW1Z80) %>%
    pivot_longer(c(SA0Z40:SW1Z80), names_to = "fap225", values_to = "nb_proj_sais") %>%
    mutate(fap225 = str_sub(fap225, 2, 6))

  # jointure et export
  df_bmo_init_proj_tot %>%
    left_join(df_bmo_init_proj_diff, by = c("siret", "fap225")) %>%
    left_join(df_bmo_init_proj_sais, by = c("siret", "fap225")) %>%
    write_rds(glue("data-raw/inter/df_bmo_init_proj_{annee}.rds"))
}