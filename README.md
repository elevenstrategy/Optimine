# Henri Bouxin le 12/09/2019

# Optimizer
Optimizer est un startup studio réalisé pour le compte de Total (https://www.optimizer.total.com/fr/notre-solution-optimizer) qui a consisté à développé puis commercialisé une solution de pilotage énergétique pour des clients de Total. Plus de détails sur la solution sur le lien suivant (https://www.youtube.com/watch?v=EZtg_QPFFSg&feature=youtu.be). 

Dans le cadre de cette solution, une solution de géolocalisation des véhicules consommateurs a été mise en place afin (1) d'animer un concours écoconduite auprès des chauffeurs et (2) de constuire des KPI sur l'activité des véhicules (temps d'arrêt, nombre d'aller retours, etc.).

## Key takeawys :
- Il s'agit d'avantage d'un projet de Data Science avec la mise en place d'un pipeline de code que d'un projet de Machine Learning
- Structure du code dans une forme industrialisable avec notamment :
  - La présence de params en début de chaque script permettant de ne pas écrire en dur dans le code les paramètres
  - La structure du script autour de 3 grandes fonctions: 
    - Extract: collecte de la donnée depuis une API
    - Transform: transformation métier de la donnée et application de méthode de Data Science
    - Load: structuration des résulats pour pouvoir les charger dans une table ou les exposer sur une API 
    NB: Cette structure est très optimale notamment lorsque votre script doit être industrialisé par un prestataire technique (comme ForePaas dans mon cas) puisqu'il n'aura qu'à adapter une partie des fonctions extract et load mais ne touchera pas à la partie transform
    - Interactions avec des bases de données SQL disponibles sur un VPS OVH (extract, insert, etc.)
    - Mes apprentissages Data lors de cette mission sont disponibles dans le word "Key Takeaways Data Manager Optimizer"
  - La mise en place de table de logs 

## Objectives & Method :
- Analyser les comportements de conduite des chauffeurs en mettant en place un concours d'écoconduite (note sur l'accélaration, la déccélération et le maintien de la vitesse) dans un premier temps avec un pipeline Python "elevenmade" basé sur de la donnée collectée sur des téléphones Samsung équippés de l'application mycartracks (https://www.mycartracks.com/) et d'une application d'identification développé par notre partenaire technique APP4MOB, et dans un deuxième temps avec des balises GPS plus robustes de la société Globo (http://www.globoconnect-europe.com/) nous transmettant directement des notes d'écoconduite

- Constuire des indicateurs sur l'activité des véhicules comme le temps d'arrêt (idle time) ou le nombre d'aller retour (cycle) entre une zone de chargement et une autre de déchargement paramétrées sur la carte à l'aide de geofences

## Sructure of the repository :
- Ecodriving : 
  - MyCarTracks: 
    - le script 0 tourne chaque jour (action automatique codé sur le CRON du serveur) pour (1) récupérer avec une API les données de traces GPS collectées sur l'applicatio mycartracks et (2) les stocker dans les bases de données OVH
    - le script 1.1 tourne chaque semaine (action automatique codé sur le CRON du serveur) et appelle les autres scripts réalisant la notation de l'écoconduite (1.1 et 1.2) et l'envoi des notes aux chauffeurs par SMS (1.3)
  - Wenco : le script récupère les données de l'API WENCO, les transforment en calculant notamment la note globale sur l'ensemble des semaines puis envoie les scores aux chauffeurs avec des SMS

- Driving KPIs_idle time_and_cycle :
  - Scripts de calcul des cyles et des idle time récupérant les traces GPS avec l'API mycartracks puis transformant la donnée afin de calculer les différentes métriques

## Stack :
- Serveur OVH (VPS): paramétrage réalisé par le prestatire technique APP4Mob responsable du développement des applications androïd et des interfaces web
- Bash for Windows (https://www.windows8facile.fr/w10-activer-bash-linux/): écriture de commande linuxe pour se connecter au serveur à distance en SSH et pour paraméter des tâches planifiés (CRON)
- API de l'application mycartracks (https://www.mycartracks.com)
- API de l'application GLOBO Connect (API définie sur mesure avec le partenaire)
- API Nexmo (https://www.nexmo.com/) permettant d'envoyer des SMS
