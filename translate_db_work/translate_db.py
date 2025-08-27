import json


def translate(raw_path: str):
    with open(raw_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
            
    out_list = []

    for conversation in raw:
        conversationId = conversation.get("conversationId")
        tweets = conversation.get("tweets")[::-1]
        first_tweet = tweets[-1]
        
        for i in range(len(tweets)):
            if tweets[i].get("replyCount") == 0:
                out_list.append(walk_backward(tweets[i:], tweets[-1].get("id")))
    
    #print
    for thread in out_list:
        for tweet in thread:
            print("------------")
            print(tweet)
            print("-------\n\n")
        

def walk_backward(tweets, conversationId):
    curr = tweets[0]
    l = len(tweets)
    index = 0
    currReplyId = curr.get("inReplyToId")
    res = [curr.get("text")]
    while currReplyId != conversationId and index < l:
        curr = tweets[index]
        if (curr.get("id") == currReplyId):
            res.append(curr.get("text"))
            currReplyId = curr.get("inReplyToId")
        
        index += 1
    res.append(tweets[-1].get("text"))
    return res[::-1]


        

def determine_descendants(original_tweet, tweets):
    if len(tweets) == 1:
        if tweets[0].get("inReplyToId") == original_tweet.get("id"):
            return [original_tweet.get("text"), tweets[0].get("text")]
        return [original_tweet.get("text")]

    threads = []
    original_tweet_id = original_tweet.get("id")
    for index, tweet in enumerate(tweets):
        t_id = tweet.get("id")
        t_reply_id = tweet.get("inReplyToId")
        
        if t_reply_id == original_tweet_id:
            threads = threads + [tweet.get("text")] +  (determine_descendants(tweets[index], tweets[index:]))
    return threads

translate("./raw.json")